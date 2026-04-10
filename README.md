# 🕷 Crawler — Buscador Pessoal

## Setup

```bash
pip install -r requirements.txt
playwright install chromium

cp .env.example .env
# edite o .env com suas credenciais
```

## Banco de dados

Execute o SQL das seeds no seu Postgres:
```bash
psql -h $DB_HOST -U $DB_USER -d $DB_NAME -f seeds.sql
```

## Rodar

**Crawler leve (httpx) — começa por aqui:**
```bash
python crawler_light.py
```

Variáveis úteis para performance do crawler leve:
```bash
CONCURRENT_REQUESTS=20   # quantas URLs processar em paralelo no worker async
CRAWLER_BATCH_SIZE=40    # quantas URLs pegar da fila por iteração
EXTERNAL_LINK_PRIORITY=2 # prioridade para links externos (novos domínios)
INTERNAL_LINK_PRIORITY=-1 # prioridade para links internos (mesmo domínio)
MAX_INTERNAL_LINKS_PER_PAGE=10 # limite de links internos enfileirados por página
```

Balanceamento da fila:
- Links externos entram com prioridade maior (padrão: `2`).
- Links internos também entram na fila (padrão: `-1`) para manter domínios ricos ativos por mais tempo.
- Quando uma URL já existe na fila, apenas itens em `pending/failed` podem ser atualizados para `pending` novamente.
- URLs já `done/processing` não são reativadas por novos links, evitando loop infinito de recrawl.
- Exceção intencional: quando a fila inteira esgota, o crawler reativa somente as URLs da tabela `crawler_seeds` para iniciar um novo ciclo de coleta.

Com versão assíncrona, o recomendado é 1 worker leve com concorrência interna alta.
Exemplo inicial no Docker: `CONCURRENT_REQUESTS=20` e `CRAWLER_BATCH_SIZE=40`.

**Crawler pesado (Playwright) — pra sites JS:**
```bash
python crawler_playwright.py
```

Variável útil para estabilidade de RAM no Playwright:
```bash
PLAYWRIGHT_CONTEXT_RECYCLE_EVERY=25 # reinicia contexto/browser page a cada N URLs
```

## Estrutura

```
crawler/
├── config.py              # variáveis de ambiente
├── models/
│   ├── __init__.py
│   ├── base.py            # schema search_engine
│   ├── page.py            # páginas indexadas
│   ├── page_link.py       # grafo de links
│   ├── crawler_queue.py   # fila BFS
│   ├── crawler_seed.py    # seeds iniciais
│   └── domain_rule.py     # regras por domínio
├── crawler_light.py       # httpx, leve, rápido
├── crawler_playwright.py  # browser headless, JS
├── seeds.sql              # seeds brasileiras + fila inicial
├── requirements.txt
└── .env.example
```

## Fluxo

```
seeds.sql popula crawler_queue
    ↓
crawler_light.py pega item da fila (FOR UPDATE SKIP LOCKED)
    ↓
GET httpx → BeautifulSoup → extrai título, texto, links
    ↓
gera summary (primeiros 1000 caracteres)
    ↓
INSERT em pages + page_links
    ↓
novos links entram na crawler_queue com depth+1
```

## Monitorar progresso

```sql
-- Resumo geral
SELECT status, count(*) FROM search_engine.crawler_queue GROUP BY status;

-- Páginas indexadas
SELECT count(*) FROM search_engine.pages WHERE status = 'indexed';

-- Top domínios
SELECT domain, count(*) FROM search_engine.pages GROUP BY domain ORDER BY count DESC LIMIT 20;
```