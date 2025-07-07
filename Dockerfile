FROM python:3.11-slim-bullseye

# Met à jour les paquets et installe les dépendances build nécessaires en une seule couche
RUN apt-get update && apt-get upgrade -y && apt-get install -y --no-install-recommends \
    build-essential \
    wget \
    gcc \
    g++ \
    make \
    libc6-dev \
    libffi-dev \
    python3-dev \
    pkg-config \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Télécharge et compile la bibliothèque C TA-Lib
RUN wget http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz && \
    tar -xzf ta-lib-0.4.0-src.tar.gz && \
    cd ta-lib && \
    ./configure --prefix=/usr && \
    make && make install && \
    cd .. && rm -rf ta-lib ta-lib-0.4.0-src.tar.gz

# Recharge le cache des bibliothèques
RUN ldconfig

# Met à jour pip avant d’installer les dépendances Python
RUN pip install --upgrade pip

# Installe TA-Lib Python en précisant les chemins vers les headers et libs
RUN pip install TA-Lib==0.4.28 --global-option=build_ext --global-option="-I/usr/include" --global-option="-L/usr/lib"

# Copie et installe les dépendances Python
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/
COPY config/ ./config/
COPY models/ ./models/
COPY scripts/ ./scripts/

RUN touch src/__init__.py src/data_ingestion/__init__.py src/processing_core/__init__.py src/database/__init__.py

ENV PYTHONPATH=/app

RUN chmod +x scripts/start_kafka_consumer.sh scripts/start_bot.sh
