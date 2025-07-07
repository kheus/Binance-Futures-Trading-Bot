import logging
import psycopg2
from psycopg2 import pool
from pathlib import Path
import yaml
import json
import pandas as pd
import time
import os
import decimal
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

# Load database configuration
CONFIG_PATH = Path(__file__).parent.parent.parent / "config" / "db_config.yaml"
with open(CONFIG_PATH, "r", encoding="utf-8-sig") as f:
    config = yaml.safe_load(f)

DB_CONFIG = config["database"]["postgresql"]
HOST = DB_CONFIG["host"]
DBNAME = DB_CONFIG["database"]
USER = DB_CONFIG["user"]
PASSWORD = DB_CONFIG["password"]
PORT = DB_CONFIG.get("port", 5432)

# Initialize connection pool
try:
    connection_pool = psycopg2.pool.ThreadedConnectionPool(
        minconn=2,
        maxconn=20,
        host=HOST,
        dbname=DBNAME,
        user=USER,
        password=PASSWORD,
        port=PORT
    )
    logger.debug("[db_handler] Database connection pool initialized.")
except Exception as e:
    logger.error(f"[db_handler] Failed to initialize connection pool: {e}")
    raise

def get_db_connection():
    try:
        conn = connection_pool.getconn()
        conn.set_client_encoding('UTF8')
        logger.debug("[db_handler] Client encoding set to UTF8")
        return conn
    except Exception as e:
        logger.error(f"[db_handler] Error getting database connection: {e}")
        raise

def release_db_connection(conn):
    try:
        connection_pool.putconn(conn)
    except Exception as e:
        logger.error(f"[db_handler] Error releasing database connection: {e}")

def execute_query(query, params=None, fetch=False):
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            logger.debug(f"[execute_query] Executing query: {query[:100]}... with params: {params}")
            cur.execute(query, params)
            if fetch:
                return cur.fetchall()
            conn.commit()
    except psycopg2.Error as e:
        logger.error(f"PostgreSQL query execution error: {e.pgerror}, code: {e.pgcode}, query: {query[:100]}..., params: {params}")
        if conn:
            conn.rollback()
        raise
    except Exception as e:
        logger.error(f"Unexpected query execution error: {str(e)}, type: {type(e).__name__}, query: {query[:100]}..., params: {params}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            release_db_connection(conn)

def create_tables():
    """
    Exécute le fichier schema.sql entier dans une seule connexion/transaction.
    """
    conn = None
    try:
        schema_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "schema.sql"))
        if not os.path.exists(schema_path):
            logger.error(f"[create_tables] schema.sql not found at {schema_path}")
            raise FileNotFoundError(f"schema.sql not found at {schema_path}")

        with open(schema_path, "r", encoding="utf-8-sig") as file:
            schema_sql = file.read()

        conn = get_db_connection()
        with conn.cursor() as cur:
            logger.info("[create_tables] Executing full schema.sql...")
            cur.execute(schema_sql)
            conn.commit()
            logger.info("[create_tables] schema.sql executed successfully.")
    except psycopg2.Error as e:
        logger.error(f"[create_tables] PostgreSQL error: {e.pgerror}, code: {e.pgcode}")
        if conn:
            conn.rollback()
        raise
    except Exception as e:
        logger.error(f"[create_tables] Unexpected error: {str(e)}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            release_db_connection(conn)

def insert_or_update_order(order):
    """
    Insert or update an order in the orders table with robust order_id handling.
    """
    query_check = "SELECT order_id FROM orders WHERE order_id = %s"
    query_update = """
        UPDATE orders
        SET symbol = %s, side = %s, quantity = %s, price = %s, status = %s, timestamp = to_timestamp(%s / 1000.0), client_order_id = %s
        WHERE order_id = %s
    """
    query_insert = """
        INSERT INTO orders (order_id, symbol, side, quantity, price, status, timestamp, client_order_id)
        VALUES (%s, %s, %s, %s, %s, %s, to_timestamp(%s / 1000.0), %s)
    """
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            # Robust order_id extraction: prefer clientOrderId, fallback to orderId, then order_id
            order_id_str = str(order.get('clientOrderId') or order.get('orderId') or order.get('order_id', '')).strip()
            if not order_id_str or order_id_str.lower() == "null" or order_id_str == "0":
                logger.error(f"[insert_or_update_order] Skipping order with invalid order_id: {order}")
                return False

            cur.execute(query_check, (order_id_str,))
            exists = cur.fetchone()
            symbol = order.get('symbol')
            side = order.get('side')
            quantity = float(order.get('origQty', order.get('quantity', 0)))
            price = float(order.get('price', 0))
            status = order.get('status')
            timestamp = int(order.get('time', order.get('timestamp', 0)))
            client_order_id = str(order.get('clientOrderId', order.get('client_order_id', '')))

            if exists:
                cur.execute(query_update, (symbol, side, quantity, price, status, timestamp, client_order_id, order_id_str))
                logger.debug(f"[insert_or_update_order] Order {order_id_str} for {symbol} updated in DB.")
            else:
                cur.execute(query_insert, (order_id_str, symbol, side, quantity, price, status, timestamp, client_order_id))
                logger.debug(f"[insert_or_update_order] Order {order_id_str} for {symbol} inserted in DB.")
            conn.commit()
            return True
    except Exception as e:
        logger.error(f"[insert_or_update_order] Error inserting/updating order {order.get('orderId', order.get('clientOrderId', ''))}: {str(e)}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            release_db_connection(conn)

def insert_trade(trade_data):
    query = """
    INSERT INTO trades (order_id, symbol, side, quantity, price, exit_price, stop_loss, take_profit, timestamp, pnl, is_trailing, trade_id, status)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, to_timestamp(%s / 1000.0), %s, %s, %s, %s)
    ON CONFLICT (trade_id) DO UPDATE
    SET order_id = EXCLUDED.order_id,
        side = EXCLUDED.side,
        quantity = EXCLUDED.quantity,
        price = EXCLUDED.price,
        exit_price = EXCLUDED.exit_price,
        stop_loss = EXCLUDED.stop_loss,
        take_profit = EXCLUDED.take_profit,
        timestamp = EXCLUDED.timestamp,
        pnl = EXCLUDED.pnl,
        is_trailing = EXCLUDED.is_trailing,
        status = EXCLUDED.status
    """
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            params = (
                str(trade_data['order_id']),
                trade_data['symbol'],
                trade_data['side'],
                float(trade_data['quantity']),
                float(trade_data['price']),
                float(trade_data['exit_price']) if trade_data.get('exit_price') is not None else None,
                float(trade_data['stop_loss']) if trade_data.get('stop_loss') is not None else None,
                float(trade_data['take_profit']) if trade_data.get('take_profit') is not None else None,
                float(trade_data['timestamp']),
                float(trade_data.get('pnl', 0.0)),
                bool(trade_data.get('is_trailing', False)),
                str(trade_data['trade_id']),
                str(trade_data.get('status', 'new'))  # Default to 'new' if not provided
            )
            logger.debug(f"[insert_trade] Executing for {trade_data['symbol']}, order_id: {trade_data['order_id']}, trade_id: {trade_data['trade_id']}")
            cur.execute(query, params)
            conn.commit()
            logger.info(f"[insert_trade] Inserted/updated trade for {trade_data['symbol']}: order_id={trade_data['order_id']}, trade_id={trade_data['trade_id']}")
        return True
    except Exception as e:
        logger.error(f"[insert_trade] Error inserting trade for {trade_data['symbol']}, order_id: {trade_data['order_id']}: {str(e)}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            release_db_connection(conn)

def insert_signal(symbol, timestamp, signal_type, price, confidence_score=None, strategy=None):
    """
    Insert a signal into the signals table, skipping duplicates.
    """
    created_at = datetime.utcnow()
    query = """
    INSERT INTO signals (symbol, timestamp, signal_type, price, created_at, confidence_score, strategy)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (symbol, timestamp) DO NOTHING
    """
    try:
        timestamp_ms = int(float(timestamp))  # Validate timestamp
        if timestamp_ms < 946684800000 or timestamp_ms > 4102444800000:  # 2000 to 2100
            raise ValueError(f"Invalid timestamp for {symbol}: {timestamp_ms}")
        execute_query(
            query,
            (symbol, timestamp_ms, signal_type, float(price), created_at, confidence_score, strategy),
            fetch=False
        )
        logger.info(f"[insert_signal] Signal inserted or skipped (if duplicate) for {symbol} at {timestamp_ms}")
        return True
    except Exception as e:
        logger.error(f"[insert_signal] Error inserting signal: {str(e)}")
        raise

def insert_training_data(symbol, timestamp, indicators, market_context, prediction=None, action=None, price=None):
    query = """
    INSERT INTO training_data (symbol, timestamp, indicators, market_context, prediction, action, price, created_at)
    VALUES (%s, %s, %s::jsonb, %s::jsonb, %s, %s, %s, %s)
    ON CONFLICT (symbol, timestamp) DO NOTHING
    """
    conn = None
    try:
        timestamp_ms = int(float(timestamp))
        if timestamp_ms < 946684800000 or timestamp_ms > 4102444800000:
            raise ValueError(f"Invalid timestamp for {symbol}: {timestamp_ms}")

        indicators_json = json.dumps(indicators) if isinstance(indicators, dict) else indicators
        market_context_json = json.dumps(market_context) if isinstance(market_context, dict) else market_context
        
        # Get current time in milliseconds for created_at
        created_at_ms = int(datetime.utcnow().timestamp() * 1000)

        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(query, (
                symbol,
                timestamp_ms,
                indicators_json,
                market_context_json,
                float(prediction) if prediction is not None else None,
                action,
                float(price) if price is not None else None,
                created_at_ms  # Use millisecond timestamp
            ))
            conn.commit()
            logger.debug(f"[insert_training_data] Inserted training data for {symbol} at {timestamp_ms}")
        return True
    except Exception as e:
        logger.error(f"[insert_training_data] Error inserting training data for {symbol} at {timestamp_ms}: {str(e)}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            release_db_connection(conn)

def update_trade_on_close(symbol, order_id, exit_price, quantity, side, leverage=50):
    query = """
    UPDATE trades
    SET exit_price = %s,
        pnl = %s,
        status = 'CLOSED'
    WHERE symbol = %s AND order_id = %s::varchar
    """
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT price FROM trades WHERE symbol = %s AND order_id = %s::varchar", (symbol, str(order_id)))
            result = cur.fetchone()
            if not result:
                logger.error(f"[update_trade_on_close] No trade found for {symbol}, order_id: {order_id}")
                return False
            entry_price = float(result[0])
            if side.upper() == 'BUY':
                pnl = (exit_price - entry_price) * quantity * leverage
            else:  # SELL
                pnl = (entry_price - exit_price) * quantity * leverage
            cur.execute(query, (float(exit_price), float(pnl), symbol, str(order_id)))
            conn.commit()
            logger.info(f"[update_trade_on_close] Updated trade for {symbol}, order_id: {order_id}, exit_price: {exit_price}, pnl: {pnl}")
        return True
    except Exception as e:
        logger.error(f"[update_trade_on_close] Error updating trade for {symbol}, order_id: {order_id}: {str(e)}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            release_db_connection(conn)

def get_pending_training_data():
    """
    Retrieve training data records that are pending processing (market_direction or prediction_correct is NULL).
    """
    query = """
    SELECT id, symbol, timestamp, indicators, market_context, market_direction, 
           price_change_pct, prediction_correct, created_at, updated_at
    FROM training_data
    WHERE (market_direction IS NULL OR prediction_correct IS NULL)
    ORDER BY timestamp ASC
    LIMIT 100
    """
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(query)
            columns = [desc[0] for desc in cur.description]
            results = cur.fetchall()
            records = [dict(zip(columns, row)) for row in results]
            logger.info(f"[get_pending_training_data] Fetched {len(records)} pending training data records")
            return records
    except Exception as e:
        logger.error(f"[get_pending_training_data] Error fetching pending training data: {str(e)}")
        raise
    finally:
        if conn:
            release_db_connection(conn)

def update_training_outcome(record_id, market_direction, price_change_pct, prediction_correct):
    """
    Update a training data record with market direction, price change percentage, and prediction outcome.
    Uses millisecond timestamp for updated_at.
    """
    query = """
    UPDATE training_data
    SET market_direction = %s,
        price_change_pct = %s,
        prediction_correct = %s,
        updated_at = %s
    WHERE id = %s
    """
    try:
        updated_at_ms = int(datetime.utcnow().timestamp() * 1000)
        execute_query(query, (
            int(market_direction),
            float(price_change_pct),
            bool(prediction_correct),
            updated_at_ms,  # Use millisecond timestamp
            int(record_id)
        ))
        logger.info(f"[update_training_outcome] Updated training data record ID={record_id}")
    except Exception as e:
        logger.error(f"[update_training_outcome] Error updating training data record ID={record_id}: {str(e)}")
        raise

def get_future_prices(symbol, signal_timestamp, candle_count=5):
    """
    Fetch future price data for a given symbol and timestamp.
    The 'timestamp' column is stored as BIGINT (milliseconds).
    """
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
            df = pd.read_sql_query(query, conn, params=params)
            if not df.empty:
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            return df
    except Exception as e:
        logger.error(f"Failed to fetch future prices for {symbol}: {e}")
        raise

def get_training_data_count(symbol):
    query = """
    SELECT COUNT(*) FROM training_data
    WHERE symbol = %s
    """
    try:
        result = execute_query(query, (symbol,), fetch=True)
        return result[0][0] if result else 0
    except Exception as e:
        logger.error(f"Error fetching training data count for {symbol}: {str(e)}")
        return 0

def insert_metrics(symbol, metrics):
    query = """
    INSERT INTO metrics (symbol, timestamp, rsi, macd, adx, ema20, ema50, atr)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (symbol, timestamp) DO NOTHING
    """
    try:
        timestamp_ms = int(metrics.get('timestamp'))
        execute_query(query, (
            symbol,
            timestamp_ms,
            metrics.get('rsi'),
            metrics.get('macd'),
            metrics.get('adx'),
            metrics.get('ema20'),
            metrics.get('ema50'),
            metrics.get('atr')
        ))
        logger.info(f"Metrics inserted or skipped (if duplicate) for {symbol} at {timestamp_ms}")
        return True
    except Exception as e:
        logger.error(f"Error inserting metrics for {symbol}: {str(e)}")
        return False

def insert_price_data(price_data, symbol):
    try:
        if not isinstance(price_data, pd.DataFrame):
            logger.error(f"[insert_price_data] price_data is not a DataFrame for {symbol}: {type(price_data)}, data: {price_data}")
            return False
        required_columns = ['open', 'high', 'low', 'close', 'volume']
        if not all(col in price_data.columns for col in required_columns):
            logger.error(f"[insert_price_data] Missing required columns in price_data for {symbol}: {price_data.columns}, data: {price_data}")
            return False
        if not isinstance(price_data.index, pd.DatetimeIndex):
            logger.error(f"[insert_price_data] price_data index is not a DatetimeIndex for {symbol}: {type(price_data.index)}, data: {price_data}")
            return False
        if price_data.empty:
            logger.error(f"[insert_price_data] Empty price_data for {symbol}, data: {price_data}")
            return False
        query = """
        INSERT INTO price_data (symbol, timestamp, open, high, low, close, volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol, timestamp) DO NOTHING
        """
        timestamp_ms = int(price_data.index[-1].timestamp() * 1000)
        execute_query(query, (
            symbol,
            timestamp_ms,
            float(price_data['open'].iloc[-1]),
            float(price_data['high'].iloc[-1]),
            float(price_data['low'].iloc[-1]),
            float(price_data['close'].iloc[-1]),
            float(price_data['volume'].iloc[-1])
        ))
        logger.info(f"Price data inserted or skipped (if duplicate) for {symbol} at {timestamp_ms}")
        return True
    except Exception as e:
        logger.error(f"Error inserting price data for {symbol}: {str(e)}, data: {price_data}")
        return False

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

def get_price_history(symbol, timeframe='1h', start_timestamp=None):
    """
    Fetch historical price data for a given symbol and timeframe.
    The 'timestamp' column is stored as BIGINT (milliseconds).
    """
    timeframe_to_limit = {
        '5m': 288, '15m': 96, '1h': 168, '4h': 84, '1d': 30
    }
    limit = timeframe_to_limit.get(timeframe, 100)
    query = """
    SELECT timestamp, open, high, low, close, volume
    FROM price_data
    WHERE symbol = %s
    """
    params = [str(symbol)]
    if start_timestamp is not None:
        query += " AND timestamp >= %s"  # Direct BIGINT comparison
        params.append(int(start_timestamp))
    query += " ORDER BY timestamp DESC LIMIT %s"
    params.append(int(limit))
    try:
        with get_db_connection() as conn:
            df = pd.read_sql_query(query, conn, params=params)
            # Convert ms timestamp to datetime for returned data
            if not df.empty:
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            return df.sort_values('timestamp').to_dict(orient='records')
    except Exception as e:
        logger.error(f"Failed to fetch price history for {symbol}: {e}")
        raise

def clean_old_data(retention_days=7, trades_retention_days=None):
    """
    Delete old data from metrics and price_data tables using BIGINT ms timestamps.
    """
    try:
        conn = get_db_connection()
        conn.autocommit = True
        cursor = conn.cursor()
        threshold_ms = int((datetime.now() - timedelta(days=retention_days)).timestamp() * 1000)
        cursor.execute("DELETE FROM metrics WHERE timestamp < %s", (threshold_ms,))
        logger.info(f"[DB Cleanup] Deleted {cursor.rowcount} rows from metrics")
        cursor.execute("DELETE FROM price_data WHERE timestamp < %s", (threshold_ms,))
        logger.info(f"[DB Cleanup] Deleted {cursor.rowcount} rows from price_data")
        cursor.close()
        release_db_connection(conn)
    except Exception as e:
        logger.error(f"Failed to clean old data: {e}", exc_info=True)
        if 'conn' in locals() and conn:
            release_db_connection(conn)

def test_connection():
    conn = None
    try:
        conn = get_db_connection()
        logger.info("Successful connection to PostgreSQL.")
        print("Successful connection to PostgreSQL.")
    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL: {e}")
        print(f"Failed to connect to PostgreSQL: {e}")
    finally:
        if conn:
            release_db_connection(conn)

def get_latest_prices():
    query = "SELECT symbol, close AS price, timestamp FROM price_data ORDER BY timestamp DESC LIMIT 100"
    conn = None
    try:
        conn = get_db_connection()
        df = pd.read_sql_query(query, conn)
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].apply(lambda x: float(x) if isinstance(x, decimal.Decimal) else x)
        return df.to_dict(orient='records')
    except Exception as e:
        logger.error(f"Failed to fetch latest prices: {e}")
        raise
    finally:
        if conn:
            release_db_connection(conn)

def insert_order_if_missing(order):
    query_check = "SELECT 1 FROM orders WHERE order_id = %s AND symbol = %s"
    query_insert = """
    INSERT INTO orders (order_id, symbol, side, quantity, price, timestamp, status, client_order_id)
    VALUES (%s, %s, %s, %s, %s, to_timestamp(%s / 1000.0), %s, %s)
    """
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            params_check = (str(order['order_id']), str(order['symbol']))
            cur.execute(query_check, params_check)
            exists = cur.fetchone()
            if exists:
                logger.debug(f"Order {order['order_id']} for {order['symbol']} already exists in DB.")
                return False
            params_insert = (
                str(order['order_id']),
                str(order['symbol']),
                str(order['side']),
                float(order['quantity']),
                float(order['price']),
                int(order['timestamp']),
                str(order['status']),
                str(order.get('client_order_id', order.get('clientOrderId', None)))
            )
            cur.execute(query_insert, params_insert)
            conn.commit()
            logger.debug(f"Order {order['order_id']} for {order['symbol']} inserted in DB.")
            return True
    except Exception as e:
        logger.error(f"Failed to insert order {order['order_id']} for {order['symbol']}: {e}")
        return False
    finally:
        if conn:
            release_db_connection(conn)

def sync_orders_with_db(client, symbol_list):
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
            if insert_order_if_missing(order):
                synced_count += 1
        logger.info(f"Orders sync completed. Total: {len(all_orders)}, New: {synced_count}")
        return synced_count
    except Exception as e:
        logger.error(f"Critical sync error: {str(e)}", exc_info=True)
        return 0

def _validate_timestamp(ts):
    try:
        ts = int(float(ts))
        if ts < 946684800000 or ts > 4102444800000:
            raise ValueError(f"Timestamp {ts} hors plage valide")
        return ts
    except (TypeError, ValueError) as e:
        logger.error(f"Timestamp invalide: {ts} - {str(e)}")