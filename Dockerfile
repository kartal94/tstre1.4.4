# Temel imaj
FROM python:3.13-slim

# Sistemi güncelle ve temel paketleri kur
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    git \
    libssl-dev \
    libffi-dev \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Çalışma dizini
WORKDIR /app

# Proje dosyalarını kopyala
COPY . .

# Sanal ortam oluştur ve aktif et, pip, setuptools, wheel güncelle
RUN python3 -m venv .venv \
    && . .venv/bin/activate \
    && pip install --upgrade pip setuptools wheel \
    && pip install "https://github.com/ssut/py-googletrans/archive/refs/tags/v4.0.0.tar.gz"

# UV bağımlılıklarını yükle, lock dosyasını yok say
RUN . .venv/bin/activate \
    && uv sync --no-lock

# start.sh çalıştırılabilir yap
RUN chmod +x start.sh

# Konteyner başlatıldığında start.sh çalıştır
CMD ["/bin/bash", "-c", ". .venv/bin/activate && ./start.sh"]
