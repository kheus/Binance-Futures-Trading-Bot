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
        print(f"[DEBUG] Trying to connect to: host={db_params['host']} dbname={db_params['database']} user={db_params['user']}")
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
            if encoding.lower() != 'utf8':
                raise RuntimeError(f"Mauvais encodage PostgreSQL: {encoding}")
        
        yield conn
    except Exception as e:
        logger.error(f"DB Connection Error: {str(e).encode('utf-8', errors='replace').decode('utf-8')}")
        raise
    finally:
        if conn:
            conn.close()


def execute_query(query, params=None, fetch=False):
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            try:
                cur.execute(query, params)
                if fetch:
                    return cur.fetchall()
                else:
                    conn.commit()
            except Exception as e:
                logger.error(f"Query execution error: {e}")
                raise

def create_tables():
    try:
        with open("src/database/schema.sql", "r", encoding="utf-8-sig") as file:
            schema_sql = file.read()
        # Split les requêtes sur ';' et exécute une par une
        queries = [q.strip() for q in schema_sql.split(';') if q.strip()]
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                for query in queries:
                    try:
                        cur.execute(query)
                    except Exception as e:
                        logger.error(f"Erreur lors de l'exécution de la requête : {query[:80]}... -> {e}")
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
    """
    stop_loss = None if trade.get("is_trailing") else trade.get("stop_loss")
    params = (
        trade["order_id"],
        trade["symbol"],
        trade["side"],
        trade["quantity"],
        trade["price"],
        stop_loss,
        trade.get("take_profit"),
        trade["timestamp"],
        trade["pnl"],
        trade.get("is_trailing", False)
    )
    execute_query(query, params)
    logger.info(f"Trade insere: {trade}")

def insert_signal(signal):
    if signal.get("signal_type") not in {'buy', 'sell', 'close_buy', 'close_sell'}:
        logger.error(f"Signal type invalide: {signal}")
        return

    query = """
    INSERT INTO signals (symbol, signal_type, price, quantity, timestamp)
    VALUES (%s, %s, %s, %s, %s)
    """
    params = (
        signal["symbol"],
        signal["signal_type"],
        signal["price"],
        signal.get("quantity", 0),
        signal["timestamp"]
    )
    execute_query(query, params)
    logger.info(f"Signal insere: {signal}")

def insert_metrics(symbol, metrics):
    query = """
    INSERT INTO metrics (symbol, timestamp, rsi, macd, adx, ema20, ema50, atr)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    params = (
        symbol,
        metrics["timestamp"],
        metrics.get("rsi", 0),
        metrics.get("macd", 0),
        metrics.get("adx", 0),
        metrics.get("ema20", 0),
        metrics.get("ema50", 0),
        metrics.get("atr", 0)
    )
    execute_query(query, params)
    logger.info(f"Inserted metrics for {symbol}: {metrics}")

def insert_training_data(record):
    query = """
    INSERT INTO training_data (symbol, timestamp, prediction, action, price, indicators, market_context)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    params = (
        record['symbol'],
        record['timestamp'],
        record.get('prediction'),
        record.get('action'),
        record.get('price'),
        json.dumps(record.get('indicators', {})),
        json.dumps(record.get('market_context', {}))
    )
    execute_query(query, params)
    logger.info(f"Training data inserted: {record}")

def get_trades():
    query = "SELECT * FROM trades ORDER BY timestamp DESC"
    with get_db_connection() as conn:
        return pd.read_sql_query(query, conn).to_dict(orient='records')

def get_signals():
    query = "SELECT * FROM signals ORDER BY timestamp DESC"
    return execute_query(query, fetch=True)

def get_metrics():
    query = "SELECT * FROM metrics ORDER BY timestamp DESC"
    return execute_query(query, fetch=True)

def get_training_data_count():
    query = "SELECT COUNT(*) FROM training_data"
    result = execute_query(query, fetch=True)
    return result[0]['count'] if result else 0

def get_future_prices(symbol, signal_timestamp, candle_count=5):
    query = """
    SELECT timestamp, open, high, low, close, volume
    FROM price_data
    WHERE symbol = %s AND timestamp > %s
    ORDER BY timestamp ASC
    LIMIT %s
    """
    params = (symbol, signal_timestamp, candle_count)
    with get_db_connection() as conn:
        return pd.read_sql_query(query, conn, params=params)

def get_pending_training_data():
    five_min_ago = int(time.time() * 1000) - 300000
    query = """
    SELECT * FROM training_data
    WHERE market_direction IS NULL
    AND timestamp < %s
    """
    return execute_query(query, (five_min_ago,), fetch=True)

def update_training_outcome(record_id, market_direction, price_change_pct, prediction_correct):
    query = """
    UPDATE training_data
    SET market_direction = %s,
        price_change_pct = %s,
        prediction_correct = %s,
        updated_at = CURRENT_TIMESTAMP
    WHERE id = %s
    """
    params = (market_direction, price_change_pct, prediction_correct, record_id)
    execute_query(query, params)

def get_last_train_timestamp():
    query = "SELECT MAX(timestamp) AS max_ts FROM training_data"
    result = execute_query(query, fetch=True)
    return result[0]['max_ts'] if result and result[0]['max_ts'] is not None else 0

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
    params = (symbol, limit)

    with get_db_connection() as conn:
        return pd.read_sql_query(query, conn, params=params).sort_values('timestamp').to_dict(orient='records')

def clean_old_data(retention_days=30, trades_retention_days=90):
    current_time = int(time.time() * 1000)
    retention_ms = retention_days * 24 * 3600 * 1000
    trades_retention_ms = trades_retention_days * 24 * 3600 * 1000

    tables = ["price_data", "metrics", "signals", "training_data"]
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
 

def test_connection():
    try:
        with get_db_connection() as conn:
            logger.info("Successful connection to PostgreSQL.")
            print("Successful connection to PostgreSQL.")
    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL: {e}")
        print(f"Failed to connect to PostgreSQL: {e}")

def get_latest_prices():
    # Exemple, adapte ta requête ici
    query = "SELECT symbol, price, timestamp FROM price_data ORDER BY timestamp DESC LIMIT 100"
    with get_db_connection() as conn:
        df = pd.read_sql_query(query, conn)
    # Convertir tous les Decimal en float dans le DataFrame
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].apply(lambda x: float(x) if isinstance(x, decimal.Decimal) else x)
    return df.to_dict(orient='records')

def backfill_orders(client, symbol):
    all_orders = client.get_all_orders(symbol=symbol)
    for order in all_orders:
        # insère dans PostgreSQL si non déjà présent
        insert_metrics(symbol, order)

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
    params_check = (order['order_id'], order['symbol'])
    params_insert = (
        order['order_id'],
        order['symbol'],
        order['side'],
        order['quantity'],
        order['price'],
        order['timestamp'],
        order['status']
    )
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query_check, params_check)
                exists = cursor.fetchone()
                if exists:
                    logger.info(f"Order {order['order_id']} for {order['symbol']} already exists in DB.")
                    return False  # déjà en base
                cursor.execute(query_insert, params_insert)
                conn.commit()
                logger.info(f"Order {order['order_id']} for {order['symbol']} inserted in DB.")
                return True
    except Exception as e:
        logger.error(f"Failed to insert order {order['order_id']} for {order['symbol']}: {e}")
        return False

def sync_orders_with_db(client, symbol_list):
    """Nouvelle version corrigée pour l'API Binance"""
    try:
        create_tables()
        
        all_orders = []
        for symbol in symbol_list:
            try:
                # Utilisation de get_all_orders avec filtrage sur le statut ouvert
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

if __name__ == "__main__":
    create_tables()
    test_connection()
