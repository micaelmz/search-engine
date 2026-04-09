FROM python:3.12-slim

# Dependências do sistema necessárias para o Chromium no ARM/Debian Trixie
# playwright install-deps não funciona nesse combo, instalamos manualmente
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    curl \
    wget \
    gnupg \
    # Chromium dependencies (manual — playwright install-deps falha no Trixie ARM)
    libglib2.0-0 \
    libnss3 \
    libnspr4 \
    libdbus-1-3 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libxcb1 \
    libxkbcommon0 \
    libpango-1.0-0 \
    libcairo2 \
    libasound2 \
    libatspi2.0-0 \
    libexpat1 \
    libx11-6 \
    libx11-xcb1 \
    libxext6 \
    fonts-liberation \
    fonts-unifont \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Instala o Chromium sem tentar instalar dependências via playwright
RUN playwright install chromium

COPY . .

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]