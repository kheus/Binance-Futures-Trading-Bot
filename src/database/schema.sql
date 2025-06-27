-- Table des transactions exécutées
CREATE TABLE IF NOT EXISTS trades (
    trade_id SERIAL PRIMARY KEY,
    order_id TEXT UNIQUE NOT NULL,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('buy', 'sell')),
    quantity NUMERIC NOT NULL CHECK (quantity > 0),
    price NUMERIC NOT NULL CHECK (price > 0),
    stop_loss NUMERIC,
    take_profit NUMERIC,
    timestamp BIGINT NOT NULL,
    pnl NUMERIC DEFAULT 0.0
);

-- Table des signaux de trading
CREATE TABLE IF NOT EXISTS signals (
    signal_id SERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    signal_type TEXT NOT NULL CHECK (signal_type IN ('buy', 'sell', 'close_buy', 'close_sell')),
    price NUMERIC NOT NULL CHECK (price > 0),
    quantity NUMERIC,
    timestamp BIGINT NOT NULL
);

-- Table des indicateurs techniques calculés
CREATE TABLE IF NOT EXISTS metrics (
    metric_id SERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    timestamp BIGINT NOT NULL,
    rsi NUMERIC,
    macd NUMERIC,
    adx NUMERIC,
    ema20 NUMERIC,
    ema50 NUMERIC,
    atr NUMERIC
);

-- Table des données de training pour le modèle LSTM
CREATE TABLE IF NOT EXISTS training_data (
    id SERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    timestamp BIGINT NOT NULL,
    prediction NUMERIC,
    action TEXT,
    price NUMERIC,
    indicators JSONB,
    market_context JSONB,
    market_direction INTEGER,
    price_change_pct NUMERIC,
    prediction_correct INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table des bougies (candlestick data)
CREATE TABLE IF NOT EXISTS price_data (
    id SERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    timestamp BIGINT NOT NULL,
    open NUMERIC NOT NULL,
    high NUMERIC NOT NULL,
    low NUMERIC NOT NULL,
    close NUMERIC NOT NULL,
    volume NUMERIC NOT NULL,
    UNIQUE(symbol, timestamp)  -- Pour permettre les upserts sans doublons
);

-- Index pour améliorer les performances des requêtes temporelles
CREATE INDEX IF NOT EXISTS idx_price_data_symbol_timestamp ON price_data(symbol, timestamp);
CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades(timestamp);
CREATE INDEX IF NOT EXISTS idx_signals_timestamp ON signals(timestamp);
CREATE INDEX IF NOT EXISTS idx_metrics_timestamp ON metrics(timestamp);
CREATE INDEX IF NOT EXISTS idx_training_timestamp ON training_data(timestamp);
-- Index pour les symboles dans les tables de signaux et de métriques
CREATE INDEX IF NOT EXISTS idx_signals_symbol ON signals(symbol);
CREATE INDEX IF NOT EXISTS idx_metrics_symbol ON metrics(symbol);
CREATE INDEX IF NOT EXISTS idx_training_symbol ON training_data(symbol);