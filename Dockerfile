FROM ghcr.io/astral-sh/uv:debian-slim

ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONUNBUFFERED=1
ENV LANG=en_US.UTF-8
ENV PATH="/app/.venv/bin:$PATH"

# Temel paketleri yükle
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        bash \
        git \
        curl \
        ca-certificates \
        locales && \
    locale-gen en_US.UTF-8 && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .

# uv ile tüm bağımlılıkları kur
RUN uv sync --locked

# start.sh çalıştırılabilir yap
RUN chmod +x start.sh

CMD ["bash", "start.sh"]
