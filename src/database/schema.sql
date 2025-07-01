-- Suppression et recréation de la table metrics
DROP TABLE IF EXISTS metrics;
CREATE TABLE metrics (
    symbol TEXT NOT NULL,
    timestamp BIGINT NOT NULL,
    rsi DOUBLE PRECISION,
    macd DOUBLE PRECISION,
    adx DOUBLE PRECISION,
    ema20 DOUBLE PRECISION,
    ema50 DOUBLE PRECISION,
    atr DOUBLE PRECISION,
    PRIMARY KEY (symbol, timestamp)
);

-- Autres tables (exemples, à adapter selon votre schéma)
CREATE TABLE IF NOT EXISTS orders (
    order_id VARCHAR(64) PRIMARY KEY,
    symbol VARCHAR(20),
    side VARCHAR(10),
    quantity DECIMAL,
    price DECIMAL,
    status VARCHAR(20),
    timestamp TIMESTAMP
);
CREATE TABLE IF NOT EXISTS trades (
    id SERIAL PRIMARY KEY,
    order_id VARCHAR(64),
    symbol VARCHAR(20),
    side VARCHAR(10),
    quantity DECIMAL,
    price DECIMAL,
    stop_loss DECIMAL,
    take_profit DECIMAL,
    timestamp BIGINT,
    pnl DECIMAL,
    is_trailing BOOLEAN
);
CREATE TABLE IF NOT EXISTS price_data (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20),
    timestamp TIMESTAMP,
    open DECIMAL,
    high DECIMAL,
    low DECIMAL,
    close DECIMAL,
    volume DECIMAL,
    UNIQUE (symbol, timestamp)
);
CREATE TABLE IF NOT EXISTS metrics (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20),
    timestamp BIGINT,
    rsi DECIMAL,
    macd DECIMAL,
    adx DECIMAL,
    ema20 DECIMAL,
    ema50 DECIMAL,
    atr DECIMAL
);
CREATE TABLE IF NOT EXISTS signals (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20),
    signal_type VARCHAR(20),
    price DECIMAL,
    quantity DECIMAL,
    timestamp BIGINT
);
CREATE TABLE IF NOT EXISTS training_data (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20),
    timestamp BIGINT,
    prediction DECIMAL,
    action VARCHAR(20),
    price DECIMAL,
    indicators JSONB,
    market_context JSONB,
    market_direction INTEGER,
    price_change_pct DECIMAL,
    prediction_correct BOOLEAN,
    updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS price_history (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20),
    price DECIMAL,
    timestamp BIGINT
);

CREATE INDEX IF NOT EXISTS idx_orders_order_id ON orders(order_id);
CREATE INDEX IF NOT EXISTS idx_orders_symbol ON orders(symbol);
CREATE INDEX IF NOT EXISTS idx_price_data_symbol_timestamp ON price_data(symbol, timestamp);
CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades(timestamp);
CREATE INDEX IF NOT EXISTS idx_signals_timestamp ON signals(timestamp);
CREATE INDEX IF NOT EXISTS idx_metrics_timestamp ON metrics(timestamp);
CREATE INDEX IF NOT EXISTS idx_training_timestamp ON training_data(timestamp);
CREATE INDEX IF NOT EXISTS idx_signals_symbol ON signals(symbol);
CREATE INDEX IF NOT EXISTS idx_metrics_symbol ON metrics(symbol);
CREATE INDEX IF NOT EXISTS idx_training_symbol ON training_data(symbol);