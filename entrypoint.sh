#!/bin/bash
set -e

echo ""
echo "╔══════════════════════════════════════╗"
echo "║         🕷  Crawler Worker           ║"
echo "╚══════════════════════════════════════╝"
echo ""

# ── Valida se o crawler deve rodar ───────────────────────────────────────────

if [ "${CRAWLER_ENABLED:-false}" != "true" ]; then
  echo "⏸  CRAWLER_ENABLED != true — crawler pausado."
  echo "   Sete CRAWLER_ENABLED=true no env para iniciar."
  echo ""
  # Mantém o container vivo sem fazer nada (útil no Dokploy pra não reiniciar em loop)
  exec tail -f /dev/null
fi

echo "✅ CRAWLER_ENABLED=true — iniciando workers..."
echo ""

# ── Configurações de workers ──────────────────────────────────────────────────

CONCURRENT_REQUESTS=${CONCURRENT_REQUESTS:-${CRAWLER_CONCURRENCY:-20}}
CRAWLER_BATCH_SIZE=${CRAWLER_BATCH_SIZE:-$((CONCURRENT_REQUESTS * 2))}
PLAYWRIGHT_WORKERS=${PLAYWRIGHT_WORKERS:-1}
CRAWLER_VERBOSE_RAW=${CRAWLER_VERBOSE:-false}

case "${CRAWLER_VERBOSE_RAW,,}" in
  1|true|yes|on)
    CRAWLER_VERBOSE=true
    ;;
  *)
    CRAWLER_VERBOSE=false
    ;;
esac

VERBOSE_ARGS=()
if [ "$CRAWLER_VERBOSE" = "true" ]; then
  VERBOSE_ARGS+=("--verbose")
fi

# Compatibilidade com config.py (fallback legado)
export CRAWLER_CONCURRENCY="$CONCURRENT_REQUESTS"
export CRAWLER_BATCH_SIZE

echo "📋 Configuração:"
echo "   Worker leve async (httpx):  1"
echo "   Concurrent requests:        $CONCURRENT_REQUESTS"
echo "   Batch size:                 $CRAWLER_BATCH_SIZE"
echo "   Workers Playwright:         $PLAYWRIGHT_WORKERS"
echo "   Verbose logs:               $CRAWLER_VERBOSE"
echo "   Max depth:                  ${MAX_DEPTH:-3}"
echo "   Delay por domínio:          ${CRAWL_DELAY_MS:-1000}ms"
echo "   Watchdog timeout:           ${WATCHDOG_TIMEOUT_MINUTES:-5} mins"
echo ""

export WATCHDOG_TIMEOUT_MINUTES="${WATCHDOG_TIMEOUT_MINUTES:-5}"

# ── Sobe workers leves ────────────────────────────────────────────────────────

echo "🚀 Subindo 1 worker leve async..."
python crawler_light.py "${VERBOSE_ARGS[@]}" 2>&1 | sed "s/^/[light-1] /" &
echo "   ↳ light-worker-1 PID=$!"

# ── Sobe workers Playwright ───────────────────────────────────────────────────

echo ""
echo "🎭 Subindo $PLAYWRIGHT_WORKERS workers Playwright..."
for i in $(seq 1 $PLAYWRIGHT_WORKERS); do
  python crawler_playwright.py "${VERBOSE_ARGS[@]}" 2>&1 | sed "s/^/[playwright-$i] /" &
  echo "   ↳ playwright-worker-$i PID=$!"
  sleep 1
done

echo ""
echo "✅ Todos os workers rodando. Logs abaixo:"
echo "══════════════════════════════════════════"

# Aguarda qualquer processo filho terminar (se um morrer, o container reinicia)
wait -n

echo ""
echo "⚠️  Um worker terminou inesperadamente. Container encerrando para o Dokploy reiniciar."
exit 1