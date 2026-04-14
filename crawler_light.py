"""
Crawler leve — usa httpx puro, sem browser.
Ideal para sites que retornam conteúdo no HTML estático.
"""

import sys
import os
import asyncio
import random
import argparse
import httpx
import logging
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse, urlunparse, parse_qsl, urlencode, unquote
from bs4 import BeautifulSoup, SoupStrainer
from sqlalchemy import create_engine, select, update, func, or_
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import OperationalError

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import (
    DATABASE_URL, CRAWL_DELAY_MS, MAX_DEPTH,
    REQUEST_TIMEOUT, USER_AGENTS, MAX_LINKS_PER_DOMAIN_PER_PAGE,
    CRAWLER_CONCURRENCY, CRAWLER_BATCH_SIZE,
    MAX_INTERNAL_LINKS_PER_PAGE, EXTERNAL_LINK_PRIORITY, INTERNAL_LINK_PRIORITY,
    MAX_QUEUE_URL_LENGTH, EXCLUDED_QUEUE_DOMAINS, WATCHDOG_TIMEOUT_MINUTES,
    MAX_PAGES_PER_DOMAIN,
)
from models import Page, PageLink, CrawlerQueue, CrawlerSeed, DomainRule
from watchdog import get_heartbeat

# ── Logging ──────────────────────────────────────────────────────────────────

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
DB_DEADLOCK_RETRIES = max(1, int(os.getenv("CRAWLER_DB_DEADLOCK_RETRIES", "4")))
DB_DEADLOCK_BACKOFF_SECONDS = max(0.05, float(os.getenv("CRAWLER_DB_DEADLOCK_BACKOFF_SECONDS", "0.25")))
DB_UPSERT_CHUNK_SIZE = max(50, int(os.getenv("CRAWLER_DB_UPSERT_CHUNK_SIZE", "250")))
QUEUE_WRITE_LOCK_ENABLED = env_truthy(os.getenv("CRAWLER_QUEUE_WRITE_LOCK_ENABLED", "true"))
QUEUE_WRITE_LOCK_KEY = int(os.getenv("CRAWLER_QUEUE_WRITE_LOCK_KEY", "724001"))

logging.basicConfig(
    level=logging.DEBUG if VERBOSE else logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("crawler.light")

for noisy_logger in ("httpx", "httpcore", "urllib3", "asyncio"):
    logging.getLogger(noisy_logger).setLevel(logging.INFO if VERBOSE else logging.WARNING)

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
def log_verbose(msg):
    if VERBOSE:
        log.info(f"{CYAN}{msg}{RESET}")


def sanitize_text(value: str | None) -> str | None:
    if value is None:
        return None
    # Alguns sites retornam NUL (0x00), que quebra inserts/updates no Postgres.
    return value.replace("\x00", "")


def estimate_min_watchdog_timeout_minutes(batch_size: int, concurrency: int, request_timeout_s: int, crawl_delay_ms: int) -> int:
    """Estimativa conservadora para evitar falso-positivo do watchdog em lotes grandes."""
    safe_concurrency = max(1, concurrency)
    waves = max(1, math.ceil(batch_size / safe_concurrency))

    # Tempo de rede por "onda" + margem para parse/persistência em lote.
    wave_seconds = max(1, request_timeout_s) + max(0.0, crawl_delay_ms / 1000)
    estimated_fetch_seconds = waves * wave_seconds
    estimated_db_seconds = max(20.0, batch_size * 0.03)
    estimated_total = estimated_fetch_seconds + estimated_db_seconds

    # Margem para variação de latência/site lento sem mascarar travamentos reais.
    recommended_seconds = max(120, int(estimated_total * 2.0))
    return max(1, math.ceil(recommended_seconds / 60))


def is_deadlock_error(exc: OperationalError) -> bool:
    code = getattr(getattr(exc, "orig", None), "pgcode", None)
    if code == "40P01":
        return True
    return "deadlock detected" in str(exc).lower()


def chunk_rows(rows: list[dict], chunk_size: int) -> list[list[dict]]:
    size = max(1, chunk_size)
    return [rows[idx:idx + size] for idx in range(0, len(rows), size)]


def acquire_queue_write_lock(session: Session):
    """Serializa gravações na tabela crawler_queue para reduzir deadlocks entre workers."""
    session.execute(select(func.pg_advisory_xact_lock(QUEUE_WRITE_LOCK_KEY)))


def queue_snapshot(session: Session) -> tuple[int, int, int]:
    pending_light = session.execute(
        select(func.count()).select_from(CrawlerQueue).where(
            CrawlerQueue.status == "pending",
            or_(CrawlerQueue.needs_js.is_(False), CrawlerQueue.needs_js.is_(None)),
        )
    ).scalar_one()
    pending_js = session.execute(
        select(func.count()).select_from(CrawlerQueue).where(
            CrawlerQueue.status == "pending",
            CrawlerQueue.needs_js.is_(True),
        )
    ).scalar_one()
    processing = session.execute(
        select(func.count()).select_from(CrawlerQueue).where(CrawlerQueue.status == "processing")
    ).scalar_one()
    return pending_light, pending_js, processing


TRACKING_QUERY_KEYS = {
    "fbclid", "gclid", "dclid", "yclid", "msclkid", "mc_cid", "mc_eid",
    "_hsenc", "_hsmi", "mkt_tok", "igshid", "ref_src",
}

TRACKING_HOSTS: set[str] = {
    "www.googleadservices.com",
    "googleads.g.doubleclick.net",
    "ad.doubleclick.net",
}

SOCIAL_SHARE_PATH_PREFIXES = {
    ("facebook.com", "/dialog/share"),
    ("facebook.com", "/sharer.php"),
    ("twitter.com", "/intent/"),
    ("x.com", "/intent/"),
    ("linkedin.com", "/shareArticle"),
    ("api.whatsapp.com", "/send"),
}

NOISE_SECTION_TAGS = {"nav", "footer", "aside", "header"}
CONTENT_TEXT_TAGS = ("h1", "h2", "h3", "p", "li")

# Parse somente nós úteis para reduzir custo/memória de DOM completo.
CRAWL_SOUP_STRAINER = SoupStrainer(
    name=["title", "meta", "main", "article", "div", "a", "h1", "h2", "h3", "p", "li", "nav", "footer", "aside", "header"]
)


def is_inside_noise_section(tag) -> bool:
    parent = tag.parent
    while parent is not None:
        if parent.name in NOISE_SECTION_TAGS:
            return True
        parent = parent.parent
    return False


def get_content_roots(soup: BeautifulSoup) -> list:
    roots = []
    seen_ids = set()

    for node in soup.find_all("main"):
        node_id = id(node)
        if node_id not in seen_ids:
            seen_ids.add(node_id)
            roots.append(node)

    for node in soup.find_all("article"):
        node_id = id(node)
        if node_id not in seen_ids:
            seen_ids.add(node_id)
            roots.append(node)

    for node in soup.find_all("div", id="content"):
        node_id = id(node)
        if node_id not in seen_ids:
            seen_ids.add(node_id)
            roots.append(node)

    for node in soup.find_all("div", attrs={"role": "main"}):
        node_id = id(node)
        if node_id not in seen_ids:
            seen_ids.add(node_id)
            roots.append(node)

    return roots if roots else [soup]


def is_excluded_domain(host: str) -> bool:
    for blocked in EXCLUDED_QUEUE_DOMAINS:
        if host == blocked or host.endswith(f".{blocked}"):
            return True
    return False


def is_social_share_path(host: str, path: str) -> bool:
    host_cmp = host.lower()
    path_cmp = (path or "").lower()
    for share_host, share_prefix in SOCIAL_SHARE_PATH_PREFIXES:
        if (host_cmp == share_host or host_cmp.endswith(f".{share_host}")) and path_cmp.startswith(share_prefix):
            return True
    return False


def normalize_discovered_url(raw_url: str, base_url: str) -> str | None:
    absolute = sanitize_text(urljoin(base_url, raw_url))
    if not absolute:
        return None

    parsed = urlparse(absolute)
    if parsed.scheme not in ("http", "https"):
        return None

    host = (parsed.hostname or "").lower()
    if is_excluded_domain(host):
        return None
    if host in TRACKING_HOSTS:
        return None
    if is_social_share_path(host, parsed.path):
        return None

    if host.endswith("youtube.com") and parsed.path == "/redirect":
        target = None
        for key, value in parse_qsl(parsed.query, keep_blank_values=False):
            if key == "q" and value:
                target = unquote(value)
                break
        if not target:
            return None
        parsed = urlparse(target)
        if parsed.scheme not in ("http", "https"):
            return None
        host = (parsed.hostname or "").lower()
        if is_excluded_domain(host):
            return None
        if host in TRACKING_HOSTS:
            return None
        if is_social_share_path(host, parsed.path):
            return None

    query_pairs = []
    for key, value in parse_qsl(parsed.query, keep_blank_values=False):
        key_lower = key.lower()
        if key_lower.startswith("utm_") or key_lower in TRACKING_QUERY_KEYS:
            continue
        query_pairs.append((key, value))

    normalized = urlunparse(
        (
            parsed.scheme,
            parsed.netloc.lower(),
            parsed.path or "/",
            "",
            urlencode(query_pairs, doseq=True),
            "",
        )
    )
    normalized = sanitize_text(normalized)
    if not normalized:
        return None
    if len(normalized) > MAX_QUEUE_URL_LENGTH:
        return None
    return normalized

# ── Helpers ───────────────────────────────────────────────────────────────────

def get_domain(url: str) -> str:
    parsed = urlparse(url)
    return (parsed.hostname or parsed.netloc).lower()

def random_ua() -> str:
    return random.choice(USER_AGENTS)

def extract_text(soup: BeautifulSoup) -> tuple[str | None, str | None]:
    """Retorna (title, summary) com foco em conteúdo denso."""
    title = soup.title.string.strip() if soup.title and soup.title.string else None
    title = sanitize_text(title)

    chunks = []
    seen = set()

    for root in get_content_roots(soup):
        for tag in root.find_all(CONTENT_TEXT_TAGS):
            if is_inside_noise_section(tag):
                continue
            text = tag.get_text(separator=" ", strip=True)
            if not text:
                continue

            # Remove itens curtos de menu/lista mantendo headings informativos.
            min_len = 6 if tag.name in ("h1", "h2", "h3") else 30
            if len(text) < min_len:
                continue
            if text in seen:
                continue

            seen.add(text)
            chunks.append(text)

    content_text = " ".join(chunks)
    content_text = " ".join(content_text.split())

    summary = sanitize_text(content_text[:1000] if content_text else None)

    return title, summary

def extract_links(soup: BeautifulSoup, base_url: str) -> list[str]:
    links = []
    roots = get_content_roots(soup)

    for root in roots:
        for tag in root.find_all("a", href=True):
            if is_inside_noise_section(tag):
                continue
            href = sanitize_text(tag["href"].strip())
            if not href:
                continue
            if href.startswith(("#", "mailto:", "javascript:", "tel:")):
                continue
            clean = normalize_discovered_url(href, base_url)
            if clean:
                links.append(clean)

    if links:
        return list(set(links))

    # Fallback para páginas sem <main>/<article>/<div id=content>/<div role=main>.
    for tag in soup.find_all("a", href=True):
        if is_inside_noise_section(tag):
            continue
        href = sanitize_text(tag["href"].strip())
        if not href:
            continue
        if href.startswith(("#", "mailto:", "javascript:", "tel:")):
            continue
        clean = normalize_discovered_url(href, base_url)
        if clean:
            links.append(clean)

    return list(set(links))

# ── Robots.txt ────────────────────────────────────────────────────────────────

async def fetch_robots(domain: str, client: httpx.AsyncClient) -> str | None:
    try:
        r = await client.get(f"https://{domain}/robots.txt", timeout=5)
        if r.status_code == 200:
            return sanitize_text(r.text)
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
    links: list[str] | None = None
    needs_js: bool = False
    error: str | None = None


def claim_next_batch(session: Session, batch_size: int) -> list[QueueItem]:
    rows = session.execute(
        select(CrawlerQueue)
        .where(
            CrawlerQueue.status == "pending",
            or_(CrawlerQueue.needs_js.is_(False), CrawlerQueue.needs_js.is_(None)),
        )
        .order_by(
            CrawlerQueue.priority.desc(),
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


def mark_for_js_retry(session: Session, item_id: int):
    session.execute(
        update(CrawlerQueue)
        .where(CrawlerQueue.id == item_id)
        .values(
            status="pending",
            needs_js=True,
            attempts=CrawlerQueue.attempts + 1,
        )
    )


def filter_domains_under_cap(session: Session, domains: set[str]) -> set[str]:
    if not domains:
        return set()

    counts = dict(
        session.execute(
            select(Page.domain, func.count(Page.id))
            .where(Page.domain.in_(domains))
            .group_by(Page.domain)
        ).all()
    )
    return {domain for domain in domains if counts.get(domain, 0) < MAX_PAGES_PER_DOMAIN}


def reactivate_seeds(engine) -> int:
    """Reinsere/reativa seeds na fila quando o crawler fica sem trabalho."""
    with Session(engine) as session:
        active_count = session.execute(
            select(func.count())
            .select_from(CrawlerQueue)
            .where(CrawlerQueue.status.in_(["pending", "processing"]))
        ).scalar_one()
        if active_count > 0:
            pending_count = session.execute(
                select(func.count())
                .select_from(CrawlerQueue)
                .where(CrawlerQueue.status == "pending")
            ).scalar_one()
            return pending_count

        seeds = session.execute(
            select(CrawlerSeed.url, CrawlerSeed.priority)
        ).all()

        queue_rows = []
        for seed_url, seed_priority in seeds:
            domain = get_domain(seed_url)
            if not domain:
                continue
            queue_rows.append(
                {
                    "url": seed_url,
                    "domain": domain,
                    "priority": seed_priority or 0,
                    "depth": 0,
                    "status": "pending",
                    "needs_js": False,
                    "queued_at": datetime.now(timezone.utc),
                }
            )

        if queue_rows:
            allowed_domains = filter_domains_under_cap(
                session,
                {row["domain"] for row in queue_rows},
            )
            queue_rows = [row for row in queue_rows if row["domain"] in allowed_domains]

        if queue_rows:
            stmt = insert(CrawlerQueue.__table__).values(queue_rows)
            stmt = stmt.on_conflict_do_update(
                index_elements=["url"],
                set_={
                    "status": "pending",
                    "depth": 0,
                    "needs_js": or_(CrawlerQueue.needs_js.is_(True), stmt.excluded.needs_js.is_(True)),
                    "priority": stmt.excluded.priority,
                    "queued_at": func.now(),
                }
            )
            session.execute(stmt)
            session.commit()

        pending_count = session.execute(
            select(func.count()).select_from(CrawlerQueue).where(CrawlerQueue.status == "pending")
        ).scalar_one()
        return pending_count

def enqueue_links(session: Session, links: list[str], depth: int, source_url: str) -> tuple[int, int]:
    if depth >= MAX_DEPTH:
        log_dim(f"  depth {depth} atingiu MAX_DEPTH={MAX_DEPTH}; nenhum link será enfileirado")
        return 0, 0
    source_domain = get_domain(source_url)

    queue_rows = []
    external_links: list[tuple[str, str]] = []
    internal_links: list[tuple[str, str]] = []
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
            internal_links.append((target_url, domain))
            queue_rows.append({
                "url": target_url,
                "domain": domain,
                "priority": INTERNAL_LINK_PRIORITY,
                "depth": depth + 1,
                "status": "pending",
                "needs_js": False,
            })
            continue

        seen_for_domain = domain_counts.get(domain, 0)
        if seen_for_domain >= MAX_LINKS_PER_DOMAIN_PER_PAGE:
            continue
        domain_counts[domain] = seen_for_domain + 1
        external_links.append((target_url, domain))
        queue_rows.append({
            "url": target_url,
            "domain": domain,
            "priority": EXTERNAL_LINK_PRIORITY,
            "depth": depth + 1,
            "status": "pending",
            "needs_js": False,
        })

    if queue_rows:
        allowed_domains = filter_domains_under_cap(
            session,
            {row["domain"] for row in queue_rows},
        )
        if not allowed_domains:
            return 0, 0

        queue_rows = [row for row in queue_rows if row["domain"] in allowed_domains]
        external_links = [item for item in external_links if item[1] in allowed_domains]
        internal_links = [item for item in internal_links if item[1] in allowed_domains]

    if queue_rows:
        stmt = insert(CrawlerQueue.__table__).values(queue_rows)
        stmt = stmt.on_conflict_do_update(
            index_elements=["url"],
            set_={
                "domain": stmt.excluded.domain,
                "priority": func.greatest(CrawlerQueue.priority, stmt.excluded.priority),
                "depth": func.least(CrawlerQueue.depth, stmt.excluded.depth),
                "status": "pending",
                "needs_js": or_(CrawlerQueue.needs_js.is_(True), stmt.excluded.needs_js.is_(True)),
                "queued_at": func.now(),
            },
            where=CrawlerQueue.status.in_(["pending", "failed"]),
        )
        session.execute(stmt)

    # Salva arestas do grafo
    edges = [
        {"source_url": source_url, "target_url": target_url}
        for target_url, _domain in (external_links + internal_links)
    ]
    if edges:
        stmt = insert(PageLink.__table__).values(edges)
        stmt = stmt.on_conflict_do_nothing()
        session.execute(stmt)
    return len(external_links), len(internal_links)


def plan_links_for_batch(links: list[str], depth: int, source_url: str) -> tuple[list[dict], list[dict], int, int]:
    """Prepara rows de fila/arestas em memória (sem gravar no banco)."""
    if depth >= MAX_DEPTH:
        return [], [], 0, 0

    source_domain = get_domain(source_url)
    queue_rows: list[dict] = []
    edge_rows: list[dict] = []
    external_count = 0
    internal_count = 0
    domain_counts: dict[str, int] = {}

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
            priority = INTERNAL_LINK_PRIORITY
        else:
            seen_for_domain = domain_counts.get(domain, 0)
            if seen_for_domain >= MAX_LINKS_PER_DOMAIN_PER_PAGE:
                continue
            domain_counts[domain] = seen_for_domain + 1
            external_count += 1
            priority = EXTERNAL_LINK_PRIORITY

        queue_rows.append(
            {
                "url": target_url,
                "domain": domain,
                "priority": priority,
                "depth": depth + 1,
                "status": "pending",
                "needs_js": False,
            }
        )
        edge_rows.append(
            {
                "source_url": source_url,
                "target_url": target_url,
                "domain": domain,
            }
        )

    return queue_rows, edge_rows, external_count, internal_count

# ── Core ──────────────────────────────────────────────────────────────────────

async def crawl_url_async(
    item: QueueItem,
    rule: DomainRule,
    web_client: httpx.AsyncClient,
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

            soup = BeautifulSoup(r.text, "lxml", parse_only=CRAWL_SOUP_STRAINER)
            title, summary = extract_text(soup)
            links = extract_links(soup, item.url)
            soup.decompose()

            if not summary:
                return CrawlResult(
                    item_id=item.id,
                    url=item.url,
                    domain=item.domain,
                    depth=item.depth,
                    ok=False,
                    needs_js=True,
                    error="sem_conteudo",
                )

            return CrawlResult(
                item_id=item.id,
                url=item.url,
                domain=item.domain,
                depth=item.depth,
                ok=True,
                title=title,
                summary=summary,
                links=links,
            )
        except httpx.TimeoutException:
            return CrawlResult(item_id=item.id, url=item.url, domain=item.domain, depth=item.depth, ok=False, error="timeout")
        except Exception as e:
            return CrawlResult(item_id=item.id, url=item.url, domain=item.domain, depth=item.depth, ok=False, error=str(e))


async def process_batch(
    engine,
    items: list[QueueItem],
    web_client: httpx.AsyncClient,
    heartbeat=None,
):
    if not items:
        return

    if heartbeat:
        heartbeat.update()

    with Session(engine) as session:
        domains = {i.domain for i in items}
        rules = get_or_create_domain_rules(session, domains)

        missing_robots = [d for d in domains if not rules[d].robots_txt]

    if missing_robots:
        robots_results = await asyncio.gather(*[
            fetch_robots(domain, web_client) for domain in missing_robots
        ])
        if heartbeat:
            heartbeat.update()
    else:
        robots_results = []

    if missing_robots:
        with Session(engine) as session:
            for domain, robots_txt in zip(missing_robots, robots_results):
                session.execute(
                    update(DomainRule)
                    .where(DomainRule.domain == domain)
                    .values(robots_txt=sanitize_text(robots_txt))
                )
            session.commit()

        with Session(engine) as session:
            refreshed = session.execute(
                select(DomainRule).where(DomainRule.domain.in_(domains))
            ).scalars().all()
            rules = {r.domain: r for r in refreshed}

    sem = asyncio.Semaphore(CRAWLER_CONCURRENCY)
    tasks = [
        asyncio.create_task(crawl_url_async(
            item=i,
            rule=rules.get(i.domain, DomainRule(domain=i.domain, crawl_delay_ms=CRAWL_DELAY_MS)),
            web_client=web_client,
            sem=sem,
        ))
        for i in items
    ]
    results: list[CrawlResult] = []
    heartbeat_stride = max(1, min(200, CRAWLER_CONCURRENCY // 2))
    for idx, task in enumerate(asyncio.as_completed(tasks), start=1):
        results.append(await task)
        if heartbeat and (idx % heartbeat_stride == 0 or idx == len(tasks)):
            heartbeat.update()

    for attempt in range(1, DB_DEADLOCK_RETRIES + 1):
        try:
            with Session(engine) as session:
                if heartbeat:
                    heartbeat.update()

                success_count = 0
                failed_count = 0
                should_log_details = attempt == 1

                indexed_at = datetime.now(timezone.utc)
                page_rows: list[dict] = []
                crawled_domains: set[str] = set()
                done_ids: list[int] = []
                failed_ids: list[int] = []
                js_retry_ids: list[int] = []
                link_plans: list[tuple[str, list[dict], list[dict]]] = []

                for result in results:
                    if result.ok:
                        if should_log_details:
                            log_verbose(f"[depth={result.depth}] {result.url}")
                            log_dim(f"  título: {result.title}")
                            log_dim(f"  links encontrados: {len(result.links or [])}")

                        page_rows.append(
                            {
                                "url": result.url,
                                "domain": result.domain,
                                "title": sanitize_text(result.title),
                                "summary": sanitize_text(result.summary),
                                "embedding": None,
                                "language": None,
                                "status": "indexed",
                                "indexed_at": indexed_at,
                            }
                        )
                        crawled_domains.add(result.domain)
                        done_ids.append(result.item_id)

                        queue_rows, edge_rows, _external_count, _internal_count = plan_links_for_batch(
                            result.links or [],
                            result.depth,
                            result.url,
                        )
                        link_plans.append((result.url, queue_rows, edge_rows))
                        success_count += 1
                    else:
                        if should_log_details:
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

                        if result.needs_js:
                            js_retry_ids.append(result.item_id)
                        else:
                            failed_ids.append(result.item_id)
                        failed_count += 1

                    if heartbeat and (success_count + failed_count) % 200 == 0:
                        heartbeat.update()

                if page_rows:
                    sorted_page_rows = sorted(page_rows, key=lambda row: row["url"])
                    for page_chunk in chunk_rows(sorted_page_rows, DB_UPSERT_CHUNK_SIZE):
                        stmt = insert(Page.__table__).values(page_chunk)
                        stmt = stmt.on_conflict_do_update(
                            index_elements=["url"],
                            set_={
                                "domain": stmt.excluded.domain,
                                "title": stmt.excluded.title,
                                "summary": stmt.excluded.summary,
                                "embedding": None,
                                "language": None,
                                "status": "indexed",
                                "indexed_at": stmt.excluded.indexed_at,
                                "updated_at": func.now(),
                            }
                        )
                        session.execute(stmt)
                        if heartbeat:
                            heartbeat.update()

                if crawled_domains:
                    session.execute(
                        update(DomainRule)
                        .where(DomainRule.domain.in_(sorted(crawled_domains)))
                        .values(last_crawled_at=func.now())
                    )
                    if heartbeat:
                        heartbeat.update()

                if link_plans:
                    if QUEUE_WRITE_LOCK_ENABLED:
                        acquire_queue_write_lock(session)
                        if heartbeat:
                            heartbeat.update()

                    candidate_domains = {
                        row["domain"]
                        for _url, queue_rows, _edge_rows in link_plans
                        for row in queue_rows
                    }
                    allowed_domains = filter_domains_under_cap(session, candidate_domains) if candidate_domains else set()

                    merged_queue_by_url: dict[str, dict] = {}
                    edge_pairs: set[tuple[str, str]] = set()

                    for _url, queue_rows, edge_rows in link_plans:
                        external_count = 0
                        internal_count = 0

                        for row in queue_rows:
                            if row["domain"] not in allowed_domains:
                                continue

                            if row["priority"] == EXTERNAL_LINK_PRIORITY:
                                external_count += 1
                            else:
                                internal_count += 1

                            existing = merged_queue_by_url.get(row["url"])
                            if existing is None:
                                merged_queue_by_url[row["url"]] = row.copy()
                                continue

                            existing["priority"] = max(existing["priority"], row["priority"])
                            existing["depth"] = min(existing["depth"], row["depth"])
                            existing["needs_js"] = existing["needs_js"] or row["needs_js"]

                        for edge in edge_rows:
                            if edge["domain"] not in allowed_domains:
                                continue
                            edge_pairs.add((edge["source_url"], edge["target_url"]))

                        if should_log_details:
                            log_dim(
                                "  indexado com sucesso → "
                                f"{external_count} externos (prio {EXTERNAL_LINK_PRIORITY}) + "
                                f"{internal_count} internos (prio {INTERNAL_LINK_PRIORITY})"
                            )

                    if merged_queue_by_url:
                        sorted_queue_rows = sorted(merged_queue_by_url.values(), key=lambda row: row["url"])
                        for queue_chunk in chunk_rows(sorted_queue_rows, DB_UPSERT_CHUNK_SIZE):
                            stmt = insert(CrawlerQueue.__table__).values(queue_chunk)
                            stmt = stmt.on_conflict_do_update(
                                index_elements=["url"],
                                set_={
                                    "domain": stmt.excluded.domain,
                                    "priority": func.greatest(CrawlerQueue.priority, stmt.excluded.priority),
                                    "depth": func.least(CrawlerQueue.depth, stmt.excluded.depth),
                                    "status": "pending",
                                    "needs_js": or_(CrawlerQueue.needs_js.is_(True), stmt.excluded.needs_js.is_(True)),
                                    "queued_at": func.now(),
                                },
                                where=CrawlerQueue.status.in_(["pending", "failed"]),
                            )
                            session.execute(stmt)
                            if heartbeat:
                                heartbeat.update()

                    if edge_pairs:
                        edge_rows = [
                            {"source_url": source_url, "target_url": target_url}
                            for source_url, target_url in edge_pairs
                        ]
                        edge_rows.sort(key=lambda row: (row["source_url"], row["target_url"]))
                        for edge_chunk in chunk_rows(edge_rows, DB_UPSERT_CHUNK_SIZE):
                            stmt = insert(PageLink.__table__).values(edge_chunk)
                            stmt = stmt.on_conflict_do_nothing()
                            session.execute(stmt)
                            if heartbeat:
                                heartbeat.update()

                if done_ids:
                    session.execute(
                        update(CrawlerQueue)
                        .where(CrawlerQueue.id.in_(sorted(set(done_ids))))
                        .values(status="done")
                    )
                    if heartbeat:
                        heartbeat.update()

                if js_retry_ids:
                    session.execute(
                        update(CrawlerQueue)
                        .where(CrawlerQueue.id.in_(sorted(set(js_retry_ids))))
                        .values(
                            status="pending",
                            needs_js=True,
                            attempts=CrawlerQueue.attempts + 1,
                        )
                    )
                    if heartbeat:
                        heartbeat.update()

                if failed_ids:
                    session.execute(
                        update(CrawlerQueue)
                        .where(CrawlerQueue.id.in_(sorted(set(failed_ids))))
                        .values(
                            status="failed",
                            attempts=CrawlerQueue.attempts + 1,
                        )
                    )
                    if heartbeat:
                        heartbeat.update()

                if success_count or failed_count:
                    log_info(
                        "Resumo do lote: "
                        f"{success_count} indexadas, {failed_count} falhas"
                    )

                session.commit()
                if heartbeat:
                    heartbeat.update()
            break
        except OperationalError as exc:
            if not is_deadlock_error(exc) or attempt >= DB_DEADLOCK_RETRIES:
                raise

            wait_seconds = min(5.0, DB_DEADLOCK_BACKOFF_SECONDS * (2 ** (attempt - 1)))
            log_warn(
                f"Deadlock ao persistir lote (tentativa {attempt}/{DB_DEADLOCK_RETRIES}). "
                f"Novo retry em {wait_seconds:.2f}s."
            )
            if heartbeat:
                heartbeat.update()
            await asyncio.sleep(wait_seconds)

# ── Main loop ─────────────────────────────────────────────────────────────────

async def async_main():
    log_info("🕷  Crawler leve (httpx) iniciando...")
    log_info(f"Verbose: {'ON' if VERBOSE else 'OFF'} (env CRAWLER_VERBOSE / --verbose)")
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    empty_count = 0
    empty_sleep_seconds = 10
    empty_reactivate_threshold = 6  # 6 * 10s = 60s

    log_info(f"Concorrência async: {CRAWLER_CONCURRENCY} | Lote: {CRAWLER_BATCH_SIZE}")
    log_info(
        "Fila DB lock: "
        f"{'ON' if QUEUE_WRITE_LOCK_ENABLED else 'OFF'}"
        f" (key={QUEUE_WRITE_LOCK_KEY}, chunk={DB_UPSERT_CHUNK_SIZE})"
    )
    limits = httpx.Limits(
        max_connections=max(50, CRAWLER_CONCURRENCY * 4),
        max_keepalive_connections=max(20, CRAWLER_CONCURRENCY * 2),
    )
    timeout = httpx.Timeout(REQUEST_TIMEOUT)

    async with httpx.AsyncClient(limits=limits, timeout=timeout) as web_client:
        configured_watchdog_minutes = WATCHDOG_TIMEOUT_MINUTES
        min_recommended_watchdog_minutes = estimate_min_watchdog_timeout_minutes(
            batch_size=CRAWLER_BATCH_SIZE,
            concurrency=CRAWLER_CONCURRENCY,
            request_timeout_s=REQUEST_TIMEOUT,
            crawl_delay_ms=CRAWL_DELAY_MS,
        )

        effective_watchdog_minutes = configured_watchdog_minutes
        if configured_watchdog_minutes > 0:
            effective_watchdog_minutes = max(configured_watchdog_minutes, min_recommended_watchdog_minutes)
            if effective_watchdog_minutes > configured_watchdog_minutes:
                log_warn(
                    "WATCHDOG_TIMEOUT_MINUTES baixo para o lote/concurrency atual; "
                    f"usando timeout efetivo de {effective_watchdog_minutes} min "
                    f"(env={configured_watchdog_minutes} min)."
                )

        heartbeat = get_heartbeat(effective_watchdog_minutes)
        heartbeat.start()

        while True:
            heartbeat.update()

            with Session(engine) as session:
                items = claim_next_batch(session, CRAWLER_BATCH_SIZE)

            if not items:
                empty_count += 1
                with Session(engine) as session:
                    pending_light, pending_js, processing = queue_snapshot(session)

                if pending_light > 0:
                    log_warn(
                        "Sem claim no lote, apesar de pending_light>0 "
                        f"(pending_light={pending_light}, pending_js={pending_js}, processing={processing})"
                    )

                if empty_count >= empty_reactivate_threshold:
                    log_warn("Fila vazia por ~1 min. Reativando seeds...")
                    pending_count = await asyncio.get_running_loop().run_in_executor(
                        None,
                        reactivate_seeds,
                        engine,
                    )
                    log_info(f"Seeds reativadas. Itens pendentes na fila: {pending_count}")
                    empty_count = 0

                log_warn("Fila vazia. Aguardando 10s...")
                await asyncio.sleep(empty_sleep_seconds)
                continue

            empty_count = 0

            log_info(f"\n{'─'*60}")
            log_info(f"Processando lote com {len(items)} URLs")
            try:
                await process_batch(engine, items, web_client, heartbeat=heartbeat)
            except Exception as e:
                log_err(f"Falha ao processar lote de {len(items)} URLs: {type(e).__name__}: {e}")
                try:
                    item_ids = [item.id for item in items]
                    with Session(engine) as session:
                        session.execute(
                            update(CrawlerQueue)
                            .where(
                                CrawlerQueue.id.in_(item_ids),
                                CrawlerQueue.status == "processing",
                            )
                            .values(status="pending")
                        )
                        session.commit()
                    log_warn(f"Lote devolvido para pending após erro ({len(item_ids)} URLs).")
                except Exception as rollback_err:
                    log_err(f"Falha ao devolver lote para pending: {type(rollback_err).__name__}: {rollback_err}")
                await asyncio.sleep(2)


def main():
    asyncio.run(async_main())

if __name__ == "__main__":
    main()