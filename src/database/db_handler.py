import yaml
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager
import json
import pandas as pd
import time
import os
import decimal

# Configuration du logging
os.makedirs("logs", exist_ok=True)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler('logs/db_handler_postgres.log', encoding="utf-8-sig")
file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(file_handler)
logger.addHandler(console_handler)

# Chargement de la configuration
def load_config():
    try:
        config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../config/db_config.yaml"))
        with open(config_path, "r", encoding="utf-8-sig") as f:
            return yaml.safe_load(f)
    except Exception as e:
        logger.error(f"Erreur lecture db_config.yaml: {e}")
        raise

config = load_config()
postgres_conf = config['database']['postgresql']

@contextmanager
def get_db_connection():
    conn = None
    try:
        db_params = config["database"]["postgresql"]
        logger.info(f"[DEBUG] Trying to connect to: host={db_params['host']} dbname={db_params['database']} user={db_params['user']}")
        conn = psycopg2.connect(
            dbname=db_params['database'],
            user=db_params['user'],
            password=db_params['password'],
            host=db_params['host'],
            port=db_params['port'],
            connect_timeout=10
        )
        conn.set_client_encoding('UTF8')
        
        # Test de validation d'encodage
        with conn.cursor() as cur:
            cur.execute("SHOW client_encoding;")
            encoding = cur.fetchone()[0]
            logger.info(f"[DEBUG] Client encoding: {encoding}")
            if encoding.lower() != 'utf8':
                raise RuntimeError(f"Mauvais encodage PostgreSQL: {encoding}")
        
        yield conn
    except psycopg2.Error as e:
        logger.error(f"PostgreSQL connection error: {e.pgerror}, code: {e.pgcode}")
        raise
    except Exception as e:
        logger.error(f"Unexpected connection error: {str(e)}, type: {type(e).__name__}")
        raise
    finally:
        if conn:
            conn.close()

def execute_query(query, params=None, fetch=False):
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            try:
                logger.debug(f"[execute_query] Executing query: {query[:100]}... with params: {params}")
                cur.execute(query, params)
                if fetch:
                    return cur.fetchall()
                else:
                    conn.commit()
            except psycopg2.Error as e:
                logger.error(f"PostgreSQL query execution error: {e.pgerror}, code: {e.pgcode}, query: {query[:100]}..., params: {params}")
                raise
            except Exception as e:
                logger.error(f"Unexpected query execution error: {str(e)}, type: {type(e).__name__}, query: {query[:100]}..., params: {params}")
                raise

def create_tables():
    try:
        schema_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "schema.sql"))
        with open(schema_path, "r", encoding="utf-8-sig") as file:
            schema_sql = file.read()
        queries = [q.strip() for q in schema_sql.split(';') if q.strip()]
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                for query in queries:
                    try:
                        cur.execute(query)
                    except psycopg2.Error as e:
                        logger.error(f"Erreur lors de l'exécution de la requête : {query[:80]}... -> {e.pgerror}")
                        raise
            conn.commit()
        logger.info("Tables created or verified successfully via schema.sql.")
    except Exception as e:
        logger.error(f"Error reading or executing schema.sql: {e}")
        raise

def insert_trade(trade):
    query = """
    INSERT INTO trades (order_id, symbol, side, quantity, price, stop_loss, take_profit, timestamp, pnl, is_trailing)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (order_id, symbol) DO UPDATE
    SET side = EXCLUDED.side,
        quantity = EXCLUDED.quantity,
        price = EXCLUDED.price,
        stop_loss = EXCLUDED.stop_loss,
        take_profit = EXCLUDED.take_profit,
        timestamp = EXCLUDED.timestamp,
        pnl = EXCLUDED.pnl,
        is_trailing = EXCLUDED.is_trailing
    """
    try:
        stop_loss = None if trade.get("is_trailing") else trade.get("stop_loss")
        params = (
            str(trade["order_id"]),
            str(trade["symbol"]),
            str(trade["side"]),
            float(trade["quantity"]),
            float(trade["price"]),
            str(stop_loss) if stop_loss is not None else None,
            float(trade.get("take_profit")) if trade.get("take_profit") is not None else None,
            int(trade["timestamp"]),
            float(trade.get("pnl", 0.0)),
            bool(trade.get("is_trailing", False))
        )
        execute_query(query, params)
        logger.info(f"Trade inséré/mis à jour: order_id={trade['order_id']}, symbol={trade['symbol']}")
    except Exception as e:
        logger.error(f"Failed to insert/update trade {trade.get('order_id', 'unknown')} for {trade.get('symbol', 'unknown')}: {e}")
        raise

def insert_price_data(df, symbol):
    query = """
    INSERT INTO price_data (symbol, timestamp, open, high, low, close, volume)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (symbol, timestamp) DO NOTHING
    """
    try:
        logger.info(f"[insert_price_data] DataFrame for {symbol}: {df.to_dict()}")
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                for _, row in df.iterrows():
                    if not isinstance(row['timestamp'], pd.Timestamp):
                        logger.error(f"[insert_price_data] Invalid timestamp format for {symbol}: {row['timestamp']} (type: {type(row['timestamp'])})")
                        continue
                    params = (
                        symbol,
                        row['timestamp'],
                        float(row['open']),
                        float(row['high']),
                        float(row['low']),
                        float(row['close']),
                        float(row['volume'])
                    )
                    try:
                        cur.execute(query, params)
                    except psycopg2.Error as e:
                        logger.error(f"[insert_price_data] PostgreSQL error for {symbol}: {e.pgerror}, code: {e.pgcode}")
                        raise
                conn.commit()
                logger.info(f"Inserted price data for {symbol}")
    except Exception as e:
        logger.error(f"Failed to insert price data for {symbol}: {str(e)}, type: {type(e).__name__}")
        raise

def insert_signal(signal):
    if signal.get("signal_type") not in {'buy', 'sell', 'close_buy', 'close_sell'}:
        logger.error(f"Signal type invalide: {signal}")
        return

    query = """
    INSERT INTO signals (symbol, signal_type, price, quantity, timestamp)
    VALUES (%s, %s, %s, %s, %s)
    """
    try:
        params = (
            str(signal["symbol"]),
            str(signal["signal_type"]),
            float(signal["price"]),
            float(signal.get("quantity", 0)),
            int(signal["timestamp"])
        )
        execute_query(query, params)
        logger.info(f"Signal inséré: {signal}")
    except Exception as e:
        logger.error(f"Failed to insert signal for {signal.get('symbol', 'unknown')}: {e}")
        raise

def insert_metrics(symbol, metrics):
    query = """
    INSERT INTO metrics (symbol, timestamp, rsi, macd, adx, ema20, ema50, atr)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    try:
        params = (
            str(symbol),
            int(metrics["timestamp"]),
            float(metrics.get("rsi", 0)),
            float(metrics.get("macd", 0)),
            float(metrics.get("adx", 0)),
            float(metrics.get("ema20", 0)),
            float(metrics.get("ema50", 0)),
            float(metrics.get("atr", 0))
        )
        execute_query(query, params)
        logger.info(f"Inserted metrics for {symbol}: {metrics}")
    except Exception as e:
        logger.error(f"Failed to insert metrics for {symbol}: {e}")
        raise

def insert_training_data(record):
    query = """
    INSERT INTO training_data (symbol, timestamp, prediction, action, price, indicators, market_context)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    try:
        params = (
            str(record['symbol']),
            int(record['timestamp']),
            float(record.get('prediction')) if record.get('prediction') is not None else None,
            str(record.get('action')) if record.get('action') is not None else None,
            float(record.get('price')) if record.get('price') is not None else None,
            json.dumps(record.get('indicators', {})),
            json.dumps(record.get('market_context', {}))
        )
        execute_query(query, params)
        logger.info(f"Training data inserted: {record}")
    except Exception as e:
        logger.error(f"Failed to insert training data for {record.get('symbol', 'unknown')}: {e}")
        raise

def get_trades():
    query = "SELECT * FROM trades ORDER BY timestamp DESC"
    try:
        with get_db_connection() as conn:
            return pd.read_sql_query(query, conn).to_dict(orient='records')
    except Exception as e:
        logger.error(f"Failed to fetch trades: {e}")
        raise

def get_signals():
    query = "SELECT * FROM signals ORDER BY timestamp DESC"
    try:
        return execute_query(query, fetch=True)
    except Exception as e:
        logger.error(f"Failed to fetch signals: {e}")
        raise

def get_metrics():
    query = "SELECT * FROM metrics ORDER BY timestamp DESC"
    try:
        return execute_query(query, fetch=True)
    except Exception as e:
        logger.error(f"Failed to fetch metrics: {e}")
        raise

def get_training_data_count(since_last_train=False):
    if since_last_train:
        last_train_ts = get_last_train_timestamp()
        query = "SELECT COUNT(*) FROM training_data WHERE timestamp > %s"
        params = (last_train_ts,)
    else:
        query = "SELECT COUNT(*) FROM training_data"
        params = None
    try:
        result = execute_query(query, params, fetch=True)
        return result[0]['count'] if result else 0
    except Exception as e:
        logger.error(f"Failed to get training data count: {e}")
        raise

def get_future_prices(symbol, signal_timestamp, candle_count=5):
    query = """
    SELECT timestamp, open, high, low, close, volume
    FROM price_data
    WHERE symbol = %s AND timestamp > %s
    ORDER BY timestamp ASC
    LIMIT %s
    """
    params = (str(symbol), int(signal_timestamp), int(candle_count))
    try:
        with get_db_connection() as conn:
            return pd.read_sql_query(query, conn, params=params)
    except Exception as e:
        logger.error(f"Failed to fetch future prices for {symbol}: {e}")
        raise

def get_pending_training_data():
    five_min_ago = int(time.time() * 1000) - 300000
    query = """
    SELECT * FROM training_data
    WHERE market_direction IS NULL
    AND timestamp < %s
    """
    try:
        return execute_query(query, (five_min_ago,), fetch=True)
    except Exception as e:
        logger.error(f"Failed to fetch pending training data: {e}")
        raise

def update_training_outcome(record_id, market_direction, price_change_pct, prediction_correct):
    query = """
    UPDATE training_data
    SET market_direction = %s,
        price_change_pct = %s,
        prediction_correct = %s,
        updated_at = CURRENT_TIMESTAMP
    WHERE id = %s
    """
    try:
        params = (
            int(market_direction) if market_direction is not None else None,
            float(price_change_pct) if price_change_pct is not None else None,
            bool(prediction_correct) if prediction_correct is not None else None,
            int(record_id)
        )
        execute_query(query, params)
        logger.info(f"Updated training outcome for record_id={record_id}")
    except Exception as e:
        logger.error(f"Failed to update training outcome for record_id={record_id}: {e}")
        raise

def get_last_train_timestamp():
    query = "SELECT MAX(timestamp) AS max_ts FROM training_data"
    try:
        result = execute_query(query, fetch=True)
        return result[0]['max_ts'] if result and result[0]['max_ts'] is not None else 0
    except Exception as e:
        logger.error(f"Failed to get last train timestamp: {e}")
        raise

def get_price_history(symbol, timeframe='1h'):
    timeframe_to_limit = {
        '5m': 288,
        '15m': 96,
        '1h': 168,
        '4h': 84,
        '1d': 30
    }
    limit = timeframe_to_limit.get(timeframe, 100)
    query = """
    SELECT timestamp, open, high, low, close, volume
    FROM price_data
    WHERE symbol = %s
    ORDER BY timestamp DESC
    LIMIT %s
    """
    params = (str(symbol), int(limit))
    try:
        with get_db_connection() as conn:
            return pd.read_sql_query(query, conn, params=params).sort_values('timestamp').to_dict(orient='records')
    except Exception as e:
        logger.error(f"Failed to fetch price history for {symbol}: {e}")
        raise

def clean_old_data(retention_days=30, trades_retention_days=90):
    current_time = int(time.time() * 1000)
    retention_ms = retention_days * 24 * 3600 * 1000
    trades_retention_ms = trades_retention_days * 24 * 3600 * 1000
    tables = ["price_data", "metrics", "signals", "training_data"]
    try:
        for table in tables:
            query = f"DELETE FROM {table} WHERE timestamp < %s"
            execute_query(query, (current_time - retention_ms,))
            logger.info(f"[DB Cleanup] Deleted rows from {table}")
        query = "DELETE FROM trades WHERE timestamp < %s"
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (current_time - trades_retention_ms,))
                deleted = cur.rowcount
                conn.commit()
        logger.info(f"[DB Cleanup] {deleted} rows deleted from trades")
    except Exception as e:
        logger.error(f"Failed to clean old data: {e}")
        raise

def test_connection():
    try:
        with get_db_connection() as conn:
            logger.info("Successful connection to PostgreSQL.")
            print("Successful connection to PostgreSQL.")
    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL: {e}")
        print(f"Failed to connect to PostgreSQL: {e}")

def get_latest_prices():
    query = "SELECT symbol, close AS price, timestamp FROM price_data ORDER BY timestamp DESC LIMIT 100"
    try:
        with get_db_connection() as conn:
            df = pd.read_sql_query(query, conn)
        # Convertir les Decimal en float
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].apply(lambda x: float(x) if isinstance(x, decimal.Decimal) else x)
        return df.to_dict(orient='records')
    except Exception as e:
        logger.error(f"Failed to fetch latest prices: {e}")
        raise

def insert_order_if_missing(order):
    """
    Insère un ordre dans la table orders s'il n'existe pas déjà (par order_id et symbol).
    Retourne True si inséré, False si déjà présent.
    """
    query_check = "SELECT 1 FROM orders WHERE order_id = %s AND symbol = %s"
    query_insert = """
    INSERT INTO orders (order_id, symbol, side, quantity, price, timestamp, status)
    VALUES (%s, %s, %s, %s, %s, to_timestamp(%s / 1000.0), %s)
    """
    params_check = (str(order['order_id']), str(order['symbol']))
    params_insert = (
        str(order['order_id']),
        str(order['symbol']),
        str(order['side']),
        float(order['quantity']),
        float(order['price']),
        int(order['timestamp']),
        str(order['status'])
    )
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query_check, params_check)
                exists = cursor.fetchone()
                if exists:
                    logger.info(f"Order {order['order_id']} for {order['symbol']} already exists in DB.")
                    return False
                cursor.execute(query_insert, params_insert)
                conn.commit()
                logger.info(f"Order {order['order_id']} for {order['symbol']} inserted in DB.")
                return True
    except Exception as e:
        logger.error(f"Failed to insert order {order['order_id']} for {order['symbol']}: {e}")
        return False

def sync_orders_with_db(client, symbol_list):
    """
    Synchronise les ordres de Binance avec la base de données.
    """
    try:
        create_tables()
        all_orders = []
        for symbol in symbol_list:
            try:
                orders = client.get_all_orders(symbol=symbol, limit=50)
                open_orders = [o for o in orders if o['status'] in ['NEW', 'PARTIALLY_FILLED']]
                for order in open_orders:
                    order_data = {
                        'order_id': str(order['orderId']),
                        'symbol': order['symbol'],
                        'side': order['side'].lower(),
                        'quantity': float(order['origQty']),
                        'price': float(order['price']),
                        'timestamp': order['time'],
                        'status': order['status'].lower()
                    }
                    all_orders.append(order_data)
            except Exception as e:
                logger.error(f"Error processing {symbol}: {str(e)}")

        synced_count = 0
        for order in all_orders:
            try:
                if insert_order_if_missing(order):
                    synced_count += 1
            except Exception as e:
                logger.error(f"DB insert failed for {order['order_id']}: {str(e)}")

        logger.info(f"Orders sync completed. Total: {len(all_orders)}, New: {synced_count}")
        return synced_count
    except Exception as e:
        logger.error(f"Critical sync error: {str(e)}", exc_info=True)
        return 0

def _validate_timestamp(ts):
    """Valide et convertit un timestamp en millisecondes"""
    try:
        ts = int(float(ts))  # Double conversion pour sécurité
        # Plage de validation raisonnable (années 2000-2100)
        if ts < 946684800000 or ts > 4102444800000:
            raise ValueError(f"Timestamp {ts} hors plage valide")
        return ts
    except (TypeError, ValueError) as e:
        logger.error(f"Timestamp invalide: {ts} - {str(e)}")
        raise ValueError(f"Format de timestamp invalide: {ts}") from e

if __name__ == "__main__":
    create_tables()
    test_connection()