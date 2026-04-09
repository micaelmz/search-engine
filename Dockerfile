FROM python:3.12-slim

# Dependências do sistema + Playwright
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    curl \
    wget \
    gnupg \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Instala só o Chromium do Playwright
RUN playwright install chromium
RUN playwright install-deps chromium

COPY . .

# Script de entrypoint que valida CRAWLER_ENABLED e sobe os workers
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]