FROM ghcr.io/astral-sh/uv:debian-slim

ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONUNBUFFERED=1
ENV LANG=en_US.UTF-8
ENV PATH="/app/.venv/bin:$PATH"

# Temel paketleri ve pip'i yükle
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        bash \
        git \
        curl \
        ca-certificates \
        locales \
        python3-pip && \
    locale-gen en_US.UTF-8 && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .

# pip güncelle ve googletrans-new'i direkt GitHub üzerinden kur
RUN python3 -m pip install --upgrade pip setuptools wheel
RUN python3 -m pip install "https://github.com/lushan88a/google-trans-new/archive/refs/tags/1.1.9.tar.gz"

# uv sync ile diğer bağımlılıkları kur
RUN uv sync --locked

# Çalıştırılabilir dosya izinleri
RUN chmod +x start.sh

CMD ["bash", "start.sh"]
