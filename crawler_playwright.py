"""
Crawler pesado — usa Playwright para sites que dependem de JavaScript.
Use quando o crawler leve não conseguir extrair conteúdo.
"""

import sys
import os
import time
import random
import argparse
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
    REQUEST_TIMEOUT, USER_AGENTS, MAX_LINKS_PER_DOMAIN_PER_PAGE, EMBEDDING_ENABLED,
    PLAYWRIGHT_FALLBACK_TO_PENDING, MAX_INTERNAL_LINKS_PER_PAGE,
    EXTERNAL_LINK_PRIORITY, INTERNAL_LINK_PRIORITY, PLAYWRIGHT_CONTEXT_RECYCLE_EVERY,
)
from models import Page, PageLink, CrawlerQueue, DomainRule

# ── Logging ───────────────────────────────────────────────────────────────────

def env_truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "on"}


def parse_runtime_options() -> argparse.Namespace:
    parser = argparse.ArgumentParser(add_help=True)
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Ativa logs detalhados (inclui logs por URL e debug).",
    )
    return parser.parse_args()


ARGS = parse_runtime_options()
VERBOSE = ARGS.verbose or env_truthy(os.getenv("CRAWLER_VERBOSE"))

logging.basicConfig(
    level=logging.DEBUG if VERBOSE else logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("crawler.playwright")

for noisy_logger in ("httpx", "httpcore", "urllib3", "playwright", "asyncio"):
    logging.getLogger(noisy_logger).setLevel(logging.INFO if VERBOSE else logging.WARNING)

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
def log_verbose(msg):
    if VERBOSE:
        log.info(f"{MAGENTA}{msg}{RESET}")

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

def enqueue_links(session: Session, links: list[str], depth: int, source_url: str) -> tuple[int, int]:
    if depth >= MAX_DEPTH:
        return 0, 0
    source_domain = get_domain(source_url)
    external_links = []
    internal_links = []
    domain_counts: dict[str, int] = {}
    internal_count = 0
    for target_url in links:
        domain = get_domain(target_url)
        if not domain:
            continue
        if target_url == source_url:
            continue
        if domain == source_domain:
            if internal_count >= MAX_INTERNAL_LINKS_PER_PAGE:
                continue
            internal_count += 1
            internal_links.append(target_url)
            continue
        seen_for_domain = domain_counts.get(domain, 0)
        if seen_for_domain >= MAX_LINKS_PER_DOMAIN_PER_PAGE:
            continue
        domain_counts[domain] = seen_for_domain + 1
        external_links.append(target_url)

    new_links = [
        {
            "url": u,
            "domain": get_domain(u),
            "priority": EXTERNAL_LINK_PRIORITY,
            "depth": depth + 1,
            "status": "pending",
            "needs_js": False,
        }
        for u in external_links
    ]
    new_links.extend([
        {
            "url": u,
            "domain": get_domain(u),
            "priority": INTERNAL_LINK_PRIORITY,
            "depth": depth + 1,
            "status": "pending",
            "needs_js": False,
        }
        for u in internal_links
    ])
    if new_links:
        stmt = insert(CrawlerQueue.__table__).values(new_links)
        stmt = stmt.on_conflict_do_update(
            index_elements=["url"],
            set_={
                "domain": stmt.excluded.domain,
                "priority": func.greatest(CrawlerQueue.priority, stmt.excluded.priority),
                "depth": func.least(CrawlerQueue.depth, stmt.excluded.depth),
                "status": "pending",
                "needs_js": False,
                "queued_at": func.now(),
            },
            where=CrawlerQueue.status.in_(["pending", "failed"]),
        )
        session.execute(stmt)
    edges = [{"source_url": source_url, "target_url": u} for u in external_links + internal_links]
    if edges:
        stmt = insert(PageLink.__table__).values(edges)
        stmt = stmt.on_conflict_do_nothing()
        session.execute(stmt)
    session.commit()
    return len(external_links), len(internal_links)

# ── Core Playwright ───────────────────────────────────────────────────────────

def crawl_url_playwright(url: str, depth: int, page, session: Session) -> bool:
    domain = get_domain(url)
    delay = CRAWL_DELAY_MS / 1000
    time.sleep(delay)

    try:
        log_verbose(f"[Playwright][depth={depth}] {url}")

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
            log_dim(f"  embedding: {'sim' if embedding else 'não'}")
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

        external_links_count, internal_links_count = enqueue_links(session, links, depth, url)
        session.commit()

        log_dim(
            "  indexado → "
            f"{external_links_count} externos (prio {EXTERNAL_LINK_PRIORITY}) + "
            f"{internal_links_count} internos (prio {INTERNAL_LINK_PRIORITY})"
        )
        return True

    except PlaywrightTimeout:
        log_err(f"Timeout Playwright: {url}")
        return False
    except Exception as e:
        log_err(f"Erro Playwright {url}: {e}")
        return False
    finally:
        # Navegar para about:blank ajuda a liberar memória de páginas JS pesadas.
        try:
            page.goto("about:blank", timeout=5000, wait_until="commit")
        except Exception:
            pass


def create_playwright_page(browser):
    context = browser.new_context(
        user_agent=random_ua(),
        viewport={"width": 1280, "height": 800},
        locale="pt-BR",
        service_workers="block",
    )
    page = context.new_page()
    page.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
    """)
    return context, page


def close_extra_pages(context, main_page):
    for candidate in list(context.pages):
        if candidate is main_page:
            continue
        try:
            candidate.close()
        except Exception:
            pass

# ── Main loop ─────────────────────────────────────────────────────────────────

def main():
    log_info("🎭 Crawler Playwright iniciando...")
    log_info(f"Verbose: {'ON' if VERBOSE else 'OFF'} (env CRAWLER_VERBOSE / --verbose)")
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
        context, page = create_playwright_page(browser)
        processed_since_recycle = 0

        while True:
            with Session(engine) as session:
                # Por padrão, Playwright foca apenas no backlog needs_js.
                item = next_item(session, js_only=True)
                if not item and PLAYWRIGHT_FALLBACK_TO_PENDING:
                    item = next_item(session, js_only=False)

                if not item:
                    log_warn("Fila vazia. Aguardando 10s...")
                    time.sleep(10)
                    continue

                log_info(f"\n{'─'*60}")
                log_info(f"[Playwright][depth={item.depth}] {item.url}")
                success = crawl_url_playwright(item.url, item.depth, page, session)

                if success:
                    mark_done(session, item.id)
                else:
                    # Marca como falho mas não bloqueia — o leve pode ter pegado
                    mark_failed(session, item.id)

                close_extra_pages(context, page)
                processed_since_recycle += 1
                recycle_every = max(1, PLAYWRIGHT_CONTEXT_RECYCLE_EVERY)
                if processed_since_recycle >= recycle_every:
                    log_warn(
                        f"Reciclando contexto Playwright após {processed_since_recycle} URLs para conter RAM..."
                    )
                    try:
                        page.close()
                    except Exception:
                        pass
                    try:
                        context.close()
                    except Exception:
                        pass
                    context, page = create_playwright_page(browser)
                    processed_since_recycle = 0

        browser.close()

if __name__ == "__main__":
    main()