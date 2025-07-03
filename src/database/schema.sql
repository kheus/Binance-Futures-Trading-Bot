-- TABLE : orders
CREATE TABLE IF NOT EXISTS orders (
    order_id VARCHAR(64) PRIMARY KEY,
    symbol VARCHAR(20),
    side VARCHAR(10),
    quantity DECIMAL,
    price DECIMAL,
    status VARCHAR(20),
    timestamp timestamp with time zone
);

-- TABLE : trades
CREATE TABLE IF NOT EXISTS trades (
    id SERIAL PRIMARY KEY,
    order_id VARCHAR(64),
    symbol VARCHAR(20),
    side VARCHAR(10),
    quantity DECIMAL,
    price DECIMAL,
    stop_loss DECIMAL,
    take_profit DECIMAL,
    timestamp timestamp with time zone,
    pnl DECIMAL,
    is_trailing BOOLEAN
);

-- TABLE : price_data
CREATE TABLE IF NOT EXISTS price_data (
    symbol VARCHAR(50),
    timestamp timestamp with time zone,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    PRIMARY KEY (symbol, timestamp)
);

-- TABLE : metrics
CREATE TABLE IF NOT EXISTS metrics (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20),
    timestamp BIGINT,
    rsi DECIMAL,
    macd DECIMAL,
    adx DECIMAL,
    ema20 DECIMAL,
    ema50 DECIMAL,
    atr DECIMAL,
    UNIQUE (symbol, timestamp)
);

-- TABLE : signals
CREATE TABLE IF NOT EXISTS signals (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    timestamp BIGINT NOT NULL,
    signal_type VARCHAR(10) NOT NULL,
    price NUMERIC(20, 8) NOT NULL,
    created_at BIGINT NOT NULL,
    confidence_score NUMERIC(5, 2),
    strategy VARCHAR(20),
    UNIQUE (symbol, timestamp)
);

-- TABLE : training_data
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
    updated_at BIGINT
);

-- TABLE : price_history
CREATE TABLE IF NOT EXISTS price_history (
    symbol VARCHAR(50),
    price DOUBLE PRECISION,
    timestamp BIGINT,
    PRIMARY KEY (symbol, timestamp)
);

-- Indexes recommandés
CREATE INDEX IF NOT EXISTS idx_orders_symbol ON orders(symbol);
CREATE INDEX IF NOT EXISTS idx_price_data_symbol_timestamp ON price_data(symbol, timestamp);
CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades(timestamp);
CREATE INDEX IF NOT EXISTS idx_signals_timestamp ON signals(timestamp);
CREATE INDEX IF NOT EXISTS idx_metrics_timestamp ON metrics(timestamp);
CREATE INDEX IF NOT EXISTS idx_training_timestamp ON training_data(timestamp);
CREATE INDEX IF NOT EXISTS idx_signals_symbol ON signals(symbol);
CREATE INDEX IF NOT EXISTS idx_metrics_symbol ON metrics(symbol);
CREATE INDEX IF NOT EXISTS idx_training_symbol ON training_data(symbol);
