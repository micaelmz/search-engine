"""
Crawler pesado — usa Playwright para sites que dependem de JavaScript.
Use quando o crawler leve não conseguir extrair conteúdo.
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
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
from sqlalchemy import create_engine, select, update, func
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import (
    DATABASE_URL, EMBEDDING_API_URL, EMBEDDING_API_KEY,
    EMBEDDING_MODEL, CRAWL_DELAY_MS, MAX_DEPTH,
    REQUEST_TIMEOUT, USER_AGENTS, MAX_LINKS_PER_DOMAIN_PER_PAGE, EMBEDDING_ENABLED
)
from models import Page, PageLink, CrawlerQueue, DomainRule

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("crawler.playwright")

MAGENTA = "\033[95m"
GREEN   = "\033[92m"
YELLOW  = "\033[93m"
RED     = "\033[91m"
GRAY    = "\033[90m"
RESET   = "\033[0m"

def log_info(msg):  log.info(f"{MAGENTA}{msg}{RESET}")
def log_ok(msg):    log.info(f"{GREEN}✓ {msg}{RESET}")
def log_warn(msg):  log.warning(f"{YELLOW}⚠ {msg}{RESET}")
def log_err(msg):   log.error(f"{RED}✗ {msg}{RESET}")
def log_dim(msg):   log.debug(f"{GRAY}{msg}{RESET}")

# ── Helpers (mesmos do crawler leve) ─────────────────────────────────────────

def get_domain(url: str) -> str:
    parsed = urlparse(url)
    return (parsed.hostname or parsed.netloc).lower()

def random_ua() -> str:
    return random.choice(USER_AGENTS)

def extract_text(soup: BeautifulSoup) -> tuple[str | None, str | None]:
    title = soup.title.string.strip() if soup.title and soup.title.string else None
    relevant_tags = soup.find_all(["h1", "h2", "h3", "p", "li"])
    content_text = " ".join(t.get_text(separator=" ", strip=True) for t in relevant_tags)
    content_text = " ".join(content_text.split())
    summary = content_text[:1000] if content_text else None
    return title, summary

def extract_links(soup: BeautifulSoup, base_url: str) -> list[str]:
    links = []
    for tag in soup.find_all("a", href=True):
        href = tag["href"].strip()
        if href.startswith(("#", "mailto:", "javascript:", "tel:")):
            continue
        absolute = urljoin(base_url, href)
        parsed = urlparse(absolute)
        if parsed.scheme in ("http", "https"):
            clean = parsed._replace(fragment="").geturl()
            links.append(clean)
    return list(set(links))

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
        return r.json()["embeddings"][0]
    except Exception as e:
        log_warn(f"Embedding falhou: {e}")
        return None

def next_item(session: Session, js_only: bool = True):
    query = (
        select(CrawlerQueue)
        .where(CrawlerQueue.status == "pending")
        .order_by(
            CrawlerQueue.priority.desc(),
            CrawlerQueue.queued_at.asc(),
        )
        .limit(1)
        .with_for_update(skip_locked=True)
    )
    if js_only:
        query = query.where(CrawlerQueue.needs_js.is_(True))

    item = session.execute(query).scalar_one_or_none()
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
    source_domain = get_domain(source_url)
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

    new_links = [
        {"url": u, "domain": get_domain(u), "priority": 0, "depth": depth + 1, "status": "pending"}
        for u in external_links
    ]
    if new_links:
        stmt = insert(CrawlerQueue.__table__).values(new_links)
        stmt = stmt.on_conflict_do_nothing(index_elements=["url"])
        session.execute(stmt)
    edges = [{"source_url": source_url, "target_url": u} for u in external_links]
    if edges:
        stmt = insert(PageLink.__table__).values(edges)
        stmt = stmt.on_conflict_do_nothing()
        session.execute(stmt)
    session.commit()
    return len(external_links)

# ── Core Playwright ───────────────────────────────────────────────────────────

def crawl_url_playwright(url: str, depth: int, page, session: Session) -> bool:
    domain = get_domain(url)
    delay = CRAWL_DELAY_MS / 1000
    time.sleep(delay)

    try:
        log_info(f"[Playwright][depth={depth}] {url}")

        page.set_extra_http_headers({
            "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8",
        })

        page.goto(url, timeout=REQUEST_TIMEOUT * 1000, wait_until="domcontentloaded")

        # Aguarda conteúdo renderizar
        try:
            page.wait_for_load_state("networkidle", timeout=5000)
        except PlaywrightTimeout:
            pass  # Não crítico, continua com o que tem

        html = page.content()
        soup = BeautifulSoup(html, "lxml")

        title, summary = extract_text(soup)
        links = extract_links(soup, url)

        if not summary:
            log_warn(f"Sem conteúdo mesmo com Playwright: {url}")
            return False

        log_dim(f"  título: {title}")
        log_dim(f"  links encontrados: {len(links)}")

        embedding = None
        if EMBEDDING_ENABLED:
            embedding = get_embedding(summary)
            log_ok(f"  embedding: {'sim' if embedding else 'não'}")
        else:
            log_dim("  embedding desativado")

        stmt = insert(Page.__table__).values(
            url=url,
            domain=domain,
            title=title,
            summary=summary,
            embedding=embedding,
            status="indexed",
            indexed_at=datetime.now(timezone.utc),
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=["url"],
            set_={
                "title": title,
                "summary": summary,
                "embedding": embedding,
                "status": "indexed",
                "indexed_at": datetime.now(timezone.utc),
                "updated_at": datetime.now(timezone.utc),
            }
        )
        session.execute(stmt)
        session.execute(
            update(DomainRule)
            .where(DomainRule.domain == domain)
            .values(last_crawled_at=func.now())
        )

        external_links_count = enqueue_links(session, links, depth, url)
        session.commit()

        log_ok(f"  indexado → {external_links_count} links externos enfileirados")
        return True

    except PlaywrightTimeout:
        log_err(f"Timeout Playwright: {url}")
        return False
    except Exception as e:
        log_err(f"Erro Playwright {url}: {e}")
        return False

# ── Main loop ─────────────────────────────────────────────────────────────────

def main():
    log_info("🎭 Crawler Playwright iniciando...")
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
            ]
        )
        context = browser.new_context(
            user_agent=random_ua(),
            viewport={"width": 1280, "height": 800},
            locale="pt-BR",
        )
        page = context.new_page()

        # Anti-detecção básica
        page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
        """)

        while True:
            with Session(engine) as session:
                # Tenta pegar item que precisa de JS, senão pega qualquer pending
                item = next_item(session, js_only=True)
                if not item:
                    item = next_item(session, js_only=False)

                if not item:
                    log_warn("Fila vazia. Aguardando 10s...")
                    time.sleep(10)
                    continue

                log_info(f"\n{'─'*60}")
                success = crawl_url_playwright(item.url, item.depth, page, session)

                if success:
                    mark_done(session, item.id)
                else:
                    # Marca como falho mas não bloqueia — o leve pode ter pegado
                    mark_failed(session, item.id)

        browser.close()

if __name__ == "__main__":
    main()