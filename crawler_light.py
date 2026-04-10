"""
Crawler leve — usa httpx puro, sem browser.
Ideal para sites que retornam conteúdo no HTML estático.
"""

import sys
import os
import asyncio
import time
import random
import httpx
import logging
from dataclasses import dataclass
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
    REQUEST_TIMEOUT, USER_AGENTS, MAX_LINKS_PER_DOMAIN_PER_PAGE,
    CRAWLER_CONCURRENCY, CRAWLER_BATCH_SIZE
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
    parsed = urlparse(url)
    return (parsed.hostname or parsed.netloc).lower()

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

async def get_embedding(client: httpx.AsyncClient, text: str) -> list[float] | None:
    if not text or not text.strip():
        return None
    try:
        headers = {"Content-Type": "application/json"}
        if EMBEDDING_API_KEY:
            headers["Authorization"] = f"Bearer {EMBEDDING_API_KEY}"

        r = await client.post(
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

async def fetch_robots(domain: str, client: httpx.AsyncClient) -> str | None:
    try:
        r = await client.get(f"https://{domain}/robots.txt", timeout=5)
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

@dataclass
class QueueItem:
    id: int
    url: str
    domain: str
    depth: int


@dataclass
class CrawlResult:
    item_id: int
    url: str
    domain: str
    depth: int
    ok: bool
    title: str | None = None
    summary: str | None = None
    raw_text: str | None = None
    embedding: list[float] | None = None
    links: list[str] | None = None
    needs_js: bool = False
    error: str | None = None


def claim_next_batch(session: Session, batch_size: int) -> list[QueueItem]:
    pending_by_domain = (
        select(
            CrawlerQueue.domain.label("domain"),
            func.count(CrawlerQueue.id).label("pending_count"),
        )
        .where(CrawlerQueue.status == "pending")
        .group_by(CrawlerQueue.domain)
        .subquery()
    )

    rows = session.execute(
        select(CrawlerQueue)
        .join(pending_by_domain, pending_by_domain.c.domain == CrawlerQueue.domain)
        .where(CrawlerQueue.status == "pending")
        .order_by(
            CrawlerQueue.priority.desc(),
            pending_by_domain.c.pending_count.asc(),
            CrawlerQueue.queued_at.asc(),
        )
        .limit(batch_size)
        .with_for_update(skip_locked=True)
    ).scalars().all()

    if not rows:
        return []

    now = datetime.now(timezone.utc)
    for row in rows:
        row.status = "processing"
        row.last_attempt_at = now

    session.commit()
    return [QueueItem(id=r.id, url=r.url, domain=r.domain, depth=r.depth) for r in rows]


def get_or_create_domain_rules(session: Session, domains: set[str]) -> dict[str, DomainRule]:
    if not domains:
        return {}

    existing = session.execute(
        select(DomainRule).where(DomainRule.domain.in_(domains))
    ).scalars().all()
    rules = {r.domain: r for r in existing}

    for domain in domains:
        if domain not in rules:
            rule = DomainRule(domain=domain, robots_txt=None, crawl_delay_ms=CRAWL_DELAY_MS)
            session.add(rule)
            rules[domain] = rule

    session.commit()
    return rules


def mark_done(session: Session, item_id: int):
    session.execute(
        update(CrawlerQueue)
        .where(CrawlerQueue.id == item_id)
        .values(status="done")
    )


def mark_failed(session: Session, item_id: int, needs_js: bool = False):
    values = {
        "status": "failed",
        "attempts": CrawlerQueue.attempts + 1,
    }
    if needs_js:
        values["needs_js"] = True

    session.execute(
        update(CrawlerQueue)
        .where(CrawlerQueue.id == item_id)
        .values(**values)
    )

def enqueue_links(session: Session, links: list[str], depth: int, source_url: str) -> int:
    if depth >= MAX_DEPTH:
        return 0
    source_domain = get_domain(source_url)
    new_links = []
    external_links = []
    domain_counts: dict[str, int] = {}
    for target_url in links:
        domain = get_domain(target_url)
        if not domain:
            continue
        if domain == source_domain:
            continue
        seen_for_domain = domain_counts.get(domain, 0)
        if seen_for_domain >= MAX_LINKS_PER_DOMAIN_PER_PAGE:
            continue
        domain_counts[domain] = seen_for_domain + 1
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
    return len(external_links)

# ── Core ──────────────────────────────────────────────────────────────────────

async def crawl_url_async(
    item: QueueItem,
    rule: DomainRule,
    web_client: httpx.AsyncClient,
    embedding_client: httpx.AsyncClient,
    sem: asyncio.Semaphore,
) -> CrawlResult:
    async with sem:
        if rule.blocked:
            return CrawlResult(item_id=item.id, url=item.url, domain=item.domain, depth=item.depth, ok=False, error="dominio_bloqueado")

        if is_blocked_by_robots(item.url, rule.robots_txt):
            return CrawlResult(item_id=item.id, url=item.url, domain=item.domain, depth=item.depth, ok=False, error="bloqueado_robots")

        delay = (rule.crawl_delay_ms if rule else CRAWL_DELAY_MS) / 1000
        await asyncio.sleep(delay)

        try:
            headers = {
                "User-Agent": random_ua(),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
            }
            r = await web_client.get(
                item.url,
                headers=headers,
                timeout=REQUEST_TIMEOUT,
                follow_redirects=True,
            )

            if r.status_code != 200:
                return CrawlResult(item_id=item.id, url=item.url, domain=item.domain, depth=item.depth, ok=False, error=f"http_{r.status_code}")

            content_type = r.headers.get("content-type", "")
            if "text/html" not in content_type:
                return CrawlResult(item_id=item.id, url=item.url, domain=item.domain, depth=item.depth, ok=False, error="nao_html")

            soup = BeautifulSoup(r.text, "lxml")
            title, summary, raw_text = extract_text(soup)
            links = extract_links(soup, item.url)

            if not raw_text:
                return CrawlResult(
                    item_id=item.id,
                    url=item.url,
                    domain=item.domain,
                    depth=item.depth,
                    ok=False,
                    needs_js=True,
                    error="sem_conteudo",
                )

            embedding = await get_embedding(embedding_client, raw_text)
            return CrawlResult(
                item_id=item.id,
                url=item.url,
                domain=item.domain,
                depth=item.depth,
                ok=True,
                title=title,
                summary=summary,
                raw_text=raw_text,
                embedding=embedding,
                links=links,
            )
        except httpx.TimeoutException:
            return CrawlResult(item_id=item.id, url=item.url, domain=item.domain, depth=item.depth, ok=False, error="timeout")
        except Exception as e:
            return CrawlResult(item_id=item.id, url=item.url, domain=item.domain, depth=item.depth, ok=False, error=str(e))


async def process_batch(engine, items: list[QueueItem]):
    if not items:
        return

    with Session(engine) as session:
        domains = {i.domain for i in items}
        rules = get_or_create_domain_rules(session, domains)

        missing_robots = [d for d in domains if not rules[d].robots_txt]
    limits = httpx.Limits(max_connections=max(20, CRAWLER_CONCURRENCY * 2), max_keepalive_connections=max(10, CRAWLER_CONCURRENCY))
    timeout = httpx.Timeout(REQUEST_TIMEOUT)

    async with httpx.AsyncClient(limits=limits, timeout=timeout) as web_client:
        async with httpx.AsyncClient(limits=limits, timeout=30) as embedding_client:
            if missing_robots:
                robots_results = await asyncio.gather(*[
                    fetch_robots(domain, web_client) for domain in missing_robots
                ])
            else:
                robots_results = []

            if missing_robots:
                with Session(engine) as session:
                    for domain, robots_txt in zip(missing_robots, robots_results):
                        session.execute(
                            update(DomainRule)
                            .where(DomainRule.domain == domain)
                            .values(robots_txt=robots_txt)
                        )
                    session.commit()

                with Session(engine) as session:
                    refreshed = session.execute(
                        select(DomainRule).where(DomainRule.domain.in_(domains))
                    ).scalars().all()
                    rules = {r.domain: r for r in refreshed}

            sem = asyncio.Semaphore(CRAWLER_CONCURRENCY)
            tasks = [
                crawl_url_async(
                    item=i,
                    rule=rules.get(i.domain, DomainRule(domain=i.domain, crawl_delay_ms=CRAWL_DELAY_MS)),
                    web_client=web_client,
                    embedding_client=embedding_client,
                    sem=sem,
                )
                for i in items
            ]
            results = await asyncio.gather(*tasks)

    with Session(engine) as session:
        for result in results:
            if result.ok:
                log_info(f"[depth={result.depth}] {result.url}")
                log_dim(f"  título: {result.title}")
                log_dim(f"  links encontrados: {len(result.links or [])}")
                log_ok(f"  embedding gerado: {'sim' if result.embedding else 'não'}")

                stmt = insert(Page.__table__).values(
                    url=result.url,
                    domain=result.domain,
                    title=result.title,
                    summary=result.summary,
                    raw_text=result.raw_text,
                    embedding=result.embedding,
                    status="indexed",
                    indexed_at=datetime.now(timezone.utc),
                )
                stmt = stmt.on_conflict_do_update(
                    index_elements=["url"],
                    set_={
                        "title": result.title,
                        "summary": result.summary,
                        "raw_text": result.raw_text,
                        "embedding": result.embedding,
                        "status": "indexed",
                        "indexed_at": datetime.now(timezone.utc),
                        "updated_at": datetime.now(timezone.utc),
                    }
                )
                session.execute(stmt)

                session.execute(
                    update(DomainRule)
                    .where(DomainRule.domain == result.domain)
                    .values(last_crawled_at=func.now())
                )

                external_links_count = enqueue_links(
                    session,
                    result.links or [],
                    result.depth,
                    result.url,
                )
                mark_done(session, result.item_id)
                log_ok(f"  indexado com sucesso → {external_links_count} links externos enfileirados")
            else:
                if result.error == "dominio_bloqueado":
                    log_warn(f"Domínio bloqueado: {result.domain}")
                elif result.error == "bloqueado_robots":
                    log_warn(f"Bloqueado por robots.txt: {result.url}")
                elif result.error == "sem_conteudo":
                    log_warn(f"Sem conteúdo extraído: {result.url} — talvez precise de JS")
                elif result.error and result.error.startswith("http_"):
                    code = result.error.replace("http_", "")
                    log_warn(f"HTTP {code} → {result.url}")
                elif result.error == "nao_html":
                    log_dim(f"Ignorado (não é HTML): {result.url}")
                elif result.error == "timeout":
                    log_err(f"Timeout: {result.url}")
                else:
                    log_err(f"Erro ao crawlear {result.url}: {result.error}")

                mark_failed(session, result.item_id, needs_js=result.needs_js)

        session.commit()

# ── Main loop ─────────────────────────────────────────────────────────────────

def main():
    log_info("🕷  Crawler leve (httpx) iniciando...")
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)

    log_info(f"Concorrência async: {CRAWLER_CONCURRENCY} | Lote: {CRAWLER_BATCH_SIZE}")
    while True:
        with Session(engine) as session:
            items = claim_next_batch(session, CRAWLER_BATCH_SIZE)

        if not items:
            log_warn("Fila vazia. Aguardando 10s...")
            time.sleep(10)
            continue

        log_info(f"\n{'─'*60}")
        log_info(f"Processando lote com {len(items)} URLs")
        asyncio.run(process_batch(engine, items))

if __name__ == "__main__":
    main()