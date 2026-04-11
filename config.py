from dotenv import load_dotenv
import os

load_dotenv()


def parse_csv_env(name: str, default: str = "") -> set[str]:
    raw = os.getenv(name, default)
    return {item.strip().lower() for item in raw.split(",") if item.strip()}

# Database
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "")
DB_SCHEMA = "search_engine"

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Embedding API
EMBEDDING_API_URL = os.getenv("EMBEDDING_API_URL", "https://ai.micaelmuniz.com/api/embed")
EMBEDDING_API_KEY = os.getenv("EMBEDDING_API_KEY", "")
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "nomic-embed-text")
EMBEDDING_ENABLED = os.getenv("EMBEDDING_ENABLED", "false").lower() == "true"

# Crawler settings
CRAWL_DELAY_MS = int(os.getenv("CRAWL_DELAY_MS", "1000"))
MAX_DEPTH = int(os.getenv("MAX_DEPTH", "3"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "15"))
PLAYWRIGHT_NAVIGATION_TIMEOUT = int(os.getenv("PLAYWRIGHT_NAVIGATION_TIMEOUT", str(REQUEST_TIMEOUT)))
PLAYWRIGHT_NETWORK_IDLE_TIMEOUT_MS = int(os.getenv("PLAYWRIGHT_NETWORK_IDLE_TIMEOUT_MS", "3000"))
PLAYWRIGHT_POST_RENDER_WAIT_MS = int(os.getenv("PLAYWRIGHT_POST_RENDER_WAIT_MS", "750"))
PLAYWRIGHT_BODY_TEXT_TIMEOUT_MS = int(os.getenv("PLAYWRIGHT_BODY_TEXT_TIMEOUT_MS", "3000"))
PLAYWRIGHT_CRAWL_DELAY_MS = int(os.getenv("PLAYWRIGHT_CRAWL_DELAY_MS", "250"))
MAX_QUEUE_URL_LENGTH = int(os.getenv("MAX_QUEUE_URL_LENGTH", "1800"))
MAX_LINKS_PER_DOMAIN_PER_PAGE = int(os.getenv("MAX_LINKS_PER_DOMAIN_PER_PAGE", "3"))
MAX_INTERNAL_LINKS_PER_PAGE = int(os.getenv("MAX_INTERNAL_LINKS_PER_PAGE", "10"))
EXTERNAL_LINK_PRIORITY = int(os.getenv("EXTERNAL_LINK_PRIORITY", "2"))
INTERNAL_LINK_PRIORITY = int(os.getenv("INTERNAL_LINK_PRIORITY", "-1"))
EXCLUDED_QUEUE_DOMAINS = parse_csv_env(
    "EXCLUDED_QUEUE_DOMAINS",
    "facebook.com,instagram.com,twitter.com,x.com,linkedin.com,api.whatsapp.com,wa.me,web.whatsapp.com",
)
CRAWLER_CONCURRENCY = int(os.getenv("CONCURRENT_REQUESTS", os.getenv("CRAWLER_CONCURRENCY", "20")))
CRAWLER_BATCH_SIZE = int(os.getenv("CRAWLER_BATCH_SIZE", str(max(40, CRAWLER_CONCURRENCY * 2))))
PLAYWRIGHT_FALLBACK_TO_PENDING = os.getenv("PLAYWRIGHT_FALLBACK_TO_PENDING", "false").lower() == "true"
PLAYWRIGHT_CONTEXT_RECYCLE_EVERY = int(os.getenv("PLAYWRIGHT_CONTEXT_RECYCLE_EVERY", "25"))
WATCHDOG_TIMEOUT_MINUTES = int(os.getenv("WATCHDOG_TIMEOUT_MINUTES", "5"))

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
]