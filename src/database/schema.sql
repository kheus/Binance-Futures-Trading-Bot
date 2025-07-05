-- File: src/database/schema.sql
-- Drop unused table
DROP TABLE IF EXISTS price_history;

-- Create orders table
DROP TABLE IF EXISTS orders;
CREATE TABLE orders (
    order_id VARCHAR(64) PRIMARY KEY,
    symbol VARCHAR(20),
    side VARCHAR(10),
    quantity DECIMAL,
    price DECIMAL,
    status VARCHAR(20),
    timestamp timestamp with time zone,
    client_order_id VARCHAR(255)
);
CREATE INDEX IF NOT EXISTS idx_orders_symbol ON orders(symbol);
CREATE INDEX IF NOT EXISTS idx_orders_timestamp ON orders(timestamp);

-- Create trades table
CREATE TABLE IF NOT EXISTS trades (
    id SERIAL PRIMARY KEY,
    order_id VARCHAR(64),
    symbol VARCHAR(20),
    side VARCHAR(10),
    quantity DECIMAL,
    price DECIMAL,
    exit_price DECIMAL,  -- Added exit_price column
    stop_loss DECIMAL,
    take_profit DECIMAL,
    timestamp timestamp with time zone,
    pnl DECIMAL,
    is_trailing BOOLEAN,
    trade_id VARCHAR(64) UNIQUE NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades(timestamp);
CREATE INDEX IF NOT EXISTS idx_trades_trade_id ON trades(trade_id);

-- Migration for existing trades table (add exit_price and trade_id if missing)
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'trades' AND column_name = 'exit_price') THEN
        ALTER TABLE trades ADD COLUMN exit_price DECIMAL;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'trades' AND column_name = 'trade_id') THEN
        ALTER TABLE trades ADD COLUMN trade_id VARCHAR(64) UNIQUE;
        UPDATE trades SET trade_id = order_id WHERE trade_id IS NULL;
        ALTER TABLE trades ALTER COLUMN trade_id SET NOT NULL;
    END IF;
END $$;

-- Create price_data table
CREATE TABLE IF NOT EXISTS price_data (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20),
    timestamp timestamp with time zone,
    open DECIMAL,
    high DECIMAL,
    low DECIMAL,
    close DECIMAL,
    volume DECIMAL
);
CREATE INDEX IF NOT EXISTS idx_price_data_symbol ON price_data(symbol);
CREATE INDEX IF NOT EXISTS idx_price_data_timestamp ON price_data(timestamp);

-- Create metrics table
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
CREATE INDEX IF NOT EXISTS idx_metrics_symbol ON metrics(symbol);
CREATE INDEX IF NOT EXISTS idx_metrics_timestamp ON metrics(timestamp);

-- Create signals table
CREATE TABLE IF NOT EXISTS signals (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20),
    timestamp BIGINT,
    signal_type VARCHAR(20),
    price DECIMAL,
    created_at timestamp with time zone,
    confidence_score DECIMAL,
    strategy VARCHAR(50),
    UNIQUE(symbol, timestamp)
);
CREATE INDEX IF NOT EXISTS idx_signals_symbol ON signals(symbol);
CREATE INDEX IF NOT EXISTS idx_signals_timestamp ON signals(timestamp);

-- Create training_data table
CREATE TABLE IF NOT EXISTS training_data (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20),
    timestamp BIGINT,
    indicators JSONB,
    market_context JSONB,
    market_direction INTEGER,
    price_change_pct DECIMAL,
    prediction_correct BOOLEAN,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp with time zone,
    UNIQUE(symbol, timestamp)
);
CREATE INDEX IF NOT EXISTS idx_training_data_symbol ON training_data(symbol);
CREATE INDEX IF NOT EXISTS idx_training_data_timestamp ON training_data(timestamp);