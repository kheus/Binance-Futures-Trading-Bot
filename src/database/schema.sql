CREATE TABLE trades (
    trade_id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    side VARCHAR(10) NOT NULL,
    quantity FLOAT NOT NULL,
    price FLOAT NOT NULL,
    stop_loss FLOAT,
    take_profit FLOAT,
    timestamp BIGINT NOT NULL,
    pnl FLOAT
);

CREATE TABLE signals (
    signal_id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    signal_type VARCHAR(10) NOT NULL,
    price FLOAT NOT NULL,
    timestamp BIGINT NOT NULL
);

CREATE TABLE metrics (
    metric_id SERIAL PRIMARY KEY,
    metric_name VARCHAR(50) NOT NULL,
    value FLOAT NOT NULL,
    timestamp BIGINT NOT NULL
);
