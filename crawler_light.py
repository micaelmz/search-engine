"""
Crawler leve — usa httpx puro, sem browser.
Ideal para sites que retornam conteúdo no HTML estático.
"""

import sys
import os
import time
import random
import httpx
import logging
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, select, update, func
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import (
    DATABASE_URL, EMBEDDING_API_URL, EMBEDDING_API_KEY,
    EMBEDDING_MODEL, CRAWL_DELAY_MS, MAX_DEPTH,
    REQUEST_TIMEOUT, USER_AGENTS
)
from models import Page, PageLink, CrawlerQueue, DomainRule

# ── Logging ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("crawler.light")

CYAN   = "\033[96m"
GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
GRAY   = "\033[90m"
RESET  = "\033[0m"

def log_info(msg):    log.info(f"{CYAN}{msg}{RESET}")
def log_ok(msg):      log.info(f"{GREEN}✓ {msg}{RESET}")
def log_warn(msg):    log.warning(f"{YELLOW}⚠ {msg}{RESET}")
def log_err(msg):     log.error(f"{RED}✗ {msg}{RESET}")
def log_dim(msg):     log.debug(f"{GRAY}{msg}{RESET}")

# ── Helpers ───────────────────────────────────────────────────────────────────

def get_domain(url: str) -> str:
    return urlparse(url).netloc

def random_ua() -> str:
    return random.choice(USER_AGENTS)

def extract_text(soup: BeautifulSoup) -> tuple[str | None, str | None, str]:
    """Retorna (title, summary, raw_text)"""
    title = soup.title.string.strip() if soup.title and soup.title.string else None

    # Texto relevante: H1, H2, H3, parágrafos
    relevant_tags = soup.find_all(["h1", "h2", "h3", "p", "li"])
    raw_text = " ".join(t.get_text(separator=" ", strip=True) for t in relevant_tags)
    raw_text = " ".join(raw_text.split())  # colapsa espaços

    summary = raw_text[:500] if raw_text else None

    return title, summary, raw_text

def extract_links(soup: BeautifulSoup, base_url: str) -> list[str]:
    links = []
    for tag in soup.find_all("a", href=True):
        href = tag["href"].strip()
        if href.startswith(("#", "mailto:", "javascript:", "tel:")):
            continue
        absolute = urljoin(base_url, href)
        parsed = urlparse(absolute)
        if parsed.scheme in ("http", "https"):
            # Remove fragmento
            clean = parsed._replace(fragment="").geturl()
            links.append(clean)
    return list(set(links))

# ── Embedding ─────────────────────────────────────────────────────────────────

def get_embedding(text: str) -> list[float] | None:
    if not text or not text.strip():
        return None
    try:
        headers = {"Content-Type": "application/json"}
        if EMBEDDING_API_KEY:
            headers["Authorization"] = f"Bearer {EMBEDDING_API_KEY}"

        r = httpx.post(
            EMBEDDING_API_URL,
            json={"model": EMBEDDING_MODEL, "input": text[:4000]},
            headers=headers,
            timeout=30,
        )
        r.raise_for_status()
        data = r.json()
        return data["embeddings"][0]
    except Exception as e:
        log_warn(f"Embedding falhou: {e}")
        return None

# ── Robots.txt ────────────────────────────────────────────────────────────────

def fetch_robots(domain: str, client: httpx.Client) -> str | None:
    try:
        r = client.get(f"https://{domain}/robots.txt", timeout=5)
        if r.status_code == 200:
            return r.text
    except Exception:
        pass
    return None

def is_blocked_by_robots(url: str, robots_txt: str | None) -> bool:
    if not robots_txt:
        return False
    path = urlparse(url).path
    disallowed = []
    capture = False
    for line in robots_txt.splitlines():
        line = line.strip()
        if line.lower().startswith("user-agent:"):
            agent = line.split(":", 1)[1].strip()
            capture = agent in ("*", "Googlebot")
        elif capture and line.lower().startswith("disallow:"):
            disallowed.append(line.split(":", 1)[1].strip())
    return any(path.startswith(d) for d in disallowed if d)

# ── Fila ──────────────────────────────────────────────────────────────────────

def next_item(session: Session) -> CrawlerQueue | None:
    item = session.execute(
        select(CrawlerQueue)
        .where(CrawlerQueue.status == "pending")
        .order_by(CrawlerQueue.priority.desc(), CrawlerQueue.queued_at.asc())
        .limit(1)
        .with_for_update(skip_locked=True)
    ).scalar_one_or_none()
    if not item:
        return None
    item.status = "processing"
    item.last_attempt_at = datetime.now(timezone.utc)
    session.commit()
    return item

def mark_done(session: Session, item_id: int):
    session.execute(
        update(CrawlerQueue)
        .where(CrawlerQueue.id == item_id)
        .values(status="done")
    )
    session.commit()

def mark_failed(session: Session, item_id: int):
    session.execute(
        update(CrawlerQueue)
        .where(CrawlerQueue.id == item_id)
        .values(status="failed", attempts=CrawlerQueue.attempts + 1)
    )
    session.commit()

def enqueue_links(session: Session, links: list[str], depth: int, source_url: str) -> int:
    if depth >= MAX_DEPTH:
        return 0
    source_domain = get_domain(source_url).lower()
    new_links = []
    external_links = []
    for target_url in links:
        domain = get_domain(target_url)
        if not domain:
            continue
        if domain.lower() == source_domain:
            continue
        external_links.append(target_url)
        new_links.append({
            "url": target_url,
            "domain": domain,
            "priority": 0,
            "depth": depth + 1,
            "status": "pending",
        })

    if new_links:
        stmt = insert(CrawlerQueue.__table__).values(new_links)
        stmt = stmt.on_conflict_do_nothing(index_elements=["url"])
        session.execute(stmt)

    # Salva arestas do grafo
    edges = [{"source_url": source_url, "target_url": u} for u in external_links]
    if edges:
        stmt = insert(PageLink.__table__).values(edges)
        stmt = stmt.on_conflict_do_nothing()
        session.execute(stmt)

    session.commit()
    return len(external_links)

# ── Core ──────────────────────────────────────────────────────────────────────

def crawl_url(url: str, depth: int, client: httpx.Client, session: Session) -> bool:
    domain = get_domain(url)

    # Verifica regras do domínio
    rule = session.get(DomainRule, domain)
    if rule and rule.blocked:
        log_warn(f"Domínio bloqueado: {domain}")
        return False

    # Busca robots.txt se for primeiro acesso ao domínio
    if not rule:
        robots = fetch_robots(domain, client)
        rule = DomainRule(domain=domain, robots_txt=robots, crawl_delay_ms=CRAWL_DELAY_MS)
        session.merge(rule)
        session.commit()

    if is_blocked_by_robots(url, rule.robots_txt):
        log_warn(f"Bloqueado por robots.txt: {url}")
        return False

    # Delay de educação
    delay = (rule.crawl_delay_ms if rule else CRAWL_DELAY_MS) / 1000
    time.sleep(delay)

    try:
        headers = {
            "User-Agent": random_ua(),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        }
        r = client.get(url, headers=headers, timeout=REQUEST_TIMEOUT, follow_redirects=True)

        if r.status_code != 200:
            log_warn(f"HTTP {r.status_code} → {url}")
            return False

        content_type = r.headers.get("content-type", "")
        if "text/html" not in content_type:
            log_dim(f"Ignorado (não é HTML): {url}")
            return False

        soup = BeautifulSoup(r.text, "lxml")
        title, summary, raw_text = extract_text(soup)
        links = extract_links(soup, url)

        if not raw_text:
            log_warn(f"Sem conteúdo extraído: {url} — talvez precise de JS")
            return False

        log_info(f"[depth={depth}] {url}")
        log_dim(f"  título: {title}")
        log_dim(f"  links encontrados: {len(links)}")

        # Embedding
        embedding = get_embedding(raw_text)
        log_ok(f"  embedding gerado: {'sim' if embedding else 'não'}")

        # Upsert na tabela pages
        stmt = insert(Page.__table__).values(
            url=url,
            domain=domain,
            title=title,
            summary=summary,
            raw_text=raw_text,
            embedding=embedding,
            status="indexed",
            indexed_at=datetime.now(timezone.utc),
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=["url"],
            set_={
                "title": title,
                "summary": summary,
                "raw_text": raw_text,
                "embedding": embedding,
                "status": "indexed",
                "indexed_at": datetime.now(timezone.utc),
                "updated_at": datetime.now(timezone.utc),
            }
        )
        session.execute(stmt)

        # Atualiza last_crawled_at do domínio
        session.execute(
            update(DomainRule)
            .where(DomainRule.domain == domain)
            .values(last_crawled_at=func.now())
        )

        external_links_count = enqueue_links(session, links, depth, url)
        session.commit()

        log_ok(f"  indexado com sucesso → {external_links_count} links externos enfileirados")
        return True

    except httpx.TimeoutException:
        log_err(f"Timeout: {url}")
        return False
    except Exception as e:
        log_err(f"Erro ao crawlear {url}: {e}")
        return False

# ── Main loop ─────────────────────────────────────────────────────────────────

def main():
    log_info("🕷  Crawler leve (httpx) iniciando...")
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)

    with httpx.Client() as client:
        while True:
            with Session(engine) as session:
                item = next_item(session)

                if not item:
                    log_warn("Fila vazia. Aguardando 10s...")
                    time.sleep(10)
                    continue

                log_info(f"\n{'─'*60}")
                log_info(f"Processando: {item.url}")

                success = crawl_url(item.url, item.depth, client, session)

                if success:
                    mark_done(session, item.id)
                else:
                    mark_failed(session, item.id)

if __name__ == "__main__":
    main()