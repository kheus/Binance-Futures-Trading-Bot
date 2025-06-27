FROM python:3.11-slim-bullseye

# Update base image to latest patch version and ensure security updates
RUN apt-get update && apt-get upgrade -y && apt-get dist-upgrade -y && apt-get clean

WORKDIR /app

# Install build dependencies for TA-Lib
RUN apt-get update && apt-get upgrade -y && apt-get install -y \
    build-essential \
    wget \
    gcc \
    g++ \
    make \
    libc-dev \
    && apt-get upgrade -y \
    && rm -rf /var/lib/apt/lists/*

# Install TA-Lib system library
RUN wget http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz \
    && tar -xvzf ta-lib-0.4.0-src.tar.gz \
    && cd ta-lib \
    && ./configure --prefix=/usr \
    && make \
    && make install \
    && cd .. \
    && rm -rf ta-lib ta-lib-0.4.0-src.tar.gz

# Update library cache
RUN ldconfig

# Install TA-Lib Python module
RUN pip install TA-Lib==0.4.28 --global-option=build_ext --global-option="-I/usr/include/ta-lib" --global-option="-L/usr/lib"

# Upgrade pip
RUN pip install --upgrade pip

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/
COPY config/ ./config/
COPY models/ ./models/
COPY scripts/ ./scripts/

RUN touch src/__init__.py src/data_ingestion/__init__.py src/processing_core/__init__.py src/database/__init__.py

ENV PYTHONPATH=/app

RUN chmod +x scripts/start_kafka_consumer.sh scripts/start_bot.sh

