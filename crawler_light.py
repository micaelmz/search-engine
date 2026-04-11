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
from dataclasses import dataclass
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse, urlunparse, parse_qsl, urlencode, unquote
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, select, update, func, or_
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import (
    DATABASE_URL, EMBEDDING_API_URL, EMBEDDING_API_KEY,
    EMBEDDING_MODEL, CRAWL_DELAY_MS, MAX_DEPTH,
    REQUEST_TIMEOUT, USER_AGENTS, MAX_LINKS_PER_DOMAIN_PER_PAGE,
    CRAWLER_CONCURRENCY, CRAWLER_BATCH_SIZE, EMBEDDING_ENABLED,
    MAX_INTERNAL_LINKS_PER_PAGE, EXTERNAL_LINK_PRIORITY, INTERNAL_LINK_PRIORITY,
    MAX_QUEUE_URL_LENGTH, EXCLUDED_QUEUE_DOMAINS, WATCHDOG_TIMEOUT_MINUTES,
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

TRACKING_HOSTS = {
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
    """Retorna (title, summary)"""
    title = soup.title.string.strip() if soup.title and soup.title.string else None
    title = sanitize_text(title)

    # Texto relevante: H1, H2, H3, parágrafos
    relevant_tags = soup.find_all(["h1", "h2", "h3", "p", "li"])
    content_text = " ".join(t.get_text(separator=" ", strip=True) for t in relevant_tags)
    content_text = " ".join(content_text.split())  # colapsa espaços

    summary = sanitize_text(content_text[:1000] if content_text else None)

    return title, summary

def extract_links(soup: BeautifulSoup, base_url: str) -> list[str]:
    links = []
    for tag in soup.find_all("a", href=True):
        href = sanitize_text(tag["href"].strip())
        if not href:
            continue
        if href.startswith(("#", "mailto:", "javascript:", "tel:")):
            continue
        clean = normalize_discovered_url(href, base_url)
        if clean:
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
    embedding: list[float] | None = None
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
        external_links.append(target_url)
        queue_rows.append({
            "url": target_url,
            "domain": domain,
            "priority": EXTERNAL_LINK_PRIORITY,
            "depth": depth + 1,
            "status": "pending",
            "needs_js": False,
        })

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
    edges = [{"source_url": source_url, "target_url": u} for u in external_links + internal_links]
    if edges:
        stmt = insert(PageLink.__table__).values(edges)
        stmt = stmt.on_conflict_do_nothing()
        session.execute(stmt)
    return len(external_links), len(internal_links)

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

            embedding = None
            if EMBEDDING_ENABLED:
                embedding = await get_embedding(embedding_client, summary)
            return CrawlResult(
                item_id=item.id,
                url=item.url,
                domain=item.domain,
                depth=item.depth,
                ok=True,
                title=title,
                summary=summary,
                embedding=embedding,
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
    embedding_client: httpx.AsyncClient,
):
    if not items:
        return

    with Session(engine) as session:
        domains = {i.domain for i in items}
        rules = get_or_create_domain_rules(session, domains)

        missing_robots = [d for d in domains if not rules[d].robots_txt]

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
        success_count = 0
        failed_count = 0
        for result in results:
            if result.ok:
                log_verbose(f"[depth={result.depth}] {result.url}")
                log_dim(f"  título: {result.title}")
                log_dim(f"  links encontrados: {len(result.links or [])}")
                if EMBEDDING_ENABLED:
                    log_dim(f"  embedding gerado: {'sim' if result.embedding else 'não'}")
                else:
                    log_dim("  embedding desativado")

                stmt = insert(Page.__table__).values(
                    url=result.url,
                    domain=result.domain,
                    title=sanitize_text(result.title),
                    summary=sanitize_text(result.summary),
                    embedding=result.embedding,
                    status="indexed",
                    indexed_at=datetime.now(timezone.utc),
                )
                stmt = stmt.on_conflict_do_update(
                    index_elements=["url"],
                    set_={
                        "title": sanitize_text(result.title),
                        "summary": sanitize_text(result.summary),
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
                external_count, internal_count = external_links_count
                log_dim(
                    "  indexado com sucesso → "
                    f"{external_count} externos (prio {EXTERNAL_LINK_PRIORITY}) + "
                    f"{internal_count} internos (prio {INTERNAL_LINK_PRIORITY})"
                )
                success_count += 1
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

                if result.needs_js:
                    mark_for_js_retry(session, result.item_id)
                else:
                    mark_failed(session, result.item_id, needs_js=False)
                failed_count += 1

        if success_count or failed_count:
            log_info(
                "Resumo do lote: "
                f"{success_count} indexadas, {failed_count} falhas"
            )

        session.commit()

# ── Main loop ─────────────────────────────────────────────────────────────────

async def async_main():
    log_info("🕷  Crawler leve (httpx) iniciando...")
    log_info(f"Verbose: {'ON' if VERBOSE else 'OFF'} (env CRAWLER_VERBOSE / --verbose)")
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    empty_count = 0
    empty_sleep_seconds = 10
    empty_reactivate_threshold = 6  # 6 * 10s = 60s

    log_info(f"Concorrência async: {CRAWLER_CONCURRENCY} | Lote: {CRAWLER_BATCH_SIZE}")
    limits = httpx.Limits(
        max_connections=max(50, CRAWLER_CONCURRENCY * 4),
        max_keepalive_connections=max(20, CRAWLER_CONCURRENCY * 2),
    )
    timeout = httpx.Timeout(REQUEST_TIMEOUT)

    async with httpx.AsyncClient(limits=limits, timeout=timeout) as web_client:
        async with httpx.AsyncClient(limits=limits, timeout=30) as embedding_client:
            heartbeat = get_heartbeat(WATCHDOG_TIMEOUT_MINUTES)
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
                    await process_batch(engine, items, web_client, embedding_client)
                except Exception as e:
                    log_err(f"Falha ao processar lote de {len(items)} URLs: {type(e).__name__}: {e}")
                    await asyncio.sleep(2)


def main():
    asyncio.run(async_main())

if __name__ == "__main__":
    main()