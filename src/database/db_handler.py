import yaml
import logging
import sys
import sqlite3
from contextlib import contextmanager

# Configuration du logging
logger = logging.getLogger(__name__)

# Réglage de l'encodage UTF-8
if sys.stdout.encoding != 'UTF-8':
    sys.stdout.reconfigure(encoding='utf-8')
if sys.stderr.encoding != 'UTF-8':
    sys.stderr.reconfigure(encoding='utf-8')

# Chargement de la configuration avec encodage explicite
def load_config():
    try:
        with open("config/db_config.yaml", "r", encoding='utf-8') as f:
            config = yaml.safe_load(f)
            return config
    except UnicodeDecodeError as e:
        logger.error(f"Erreur d'encodage dans db_config.yaml: {e}")
        raise
    except Exception as e:
        logger.error(f"Erreur lecture db_config.yaml: {e}")
        raise

config = load_config()
sqlite_conf = config['database']['sqlite']

# Gestionnaire de connexion
@contextmanager
def get_db_connection():
    conn = None
    try:
        conn = sqlite3.connect(sqlite_conf['path'])
        conn.execute("PRAGMA encoding = 'UTF-8';")  # Forcer UTF-8
        yield conn
    except Exception as e:
        logger.error(f"Erreur connexion DB: {e}")
        raise
    finally:
        if conn:
            conn.close()

# Création des tables
def create_tables():
    schema = """
    CREATE TABLE IF NOT EXISTS trades (
        trade_id INTEGER PRIMARY KEY AUTOINCREMENT,
        order_id TEXT,
        symbol TEXT NOT NULL,
        side TEXT NOT NULL CHECK (side IN ('buy', 'sell')),
        quantity REAL NOT NULL CHECK (quantity > 0),
        price REAL NOT NULL CHECK (price > 0),
        stop_loss REAL,
        take_profit REAL,
        timestamp INTEGER NOT NULL,
        pnl REAL DEFAULT 0.0
    );

    CREATE TABLE IF NOT EXISTS signals (
        signal_id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT NOT NULL,
        signal_type TEXT NOT NULL CHECK (signal_type IN ('buy', 'sell', 'close_buy', 'close_sell')),
        price REAL NOT NULL CHECK (price > 0),
        quantity REAL,
        timestamp INTEGER NOT NULL
    );

    CREATE TABLE IF NOT EXISTS metrics (
        metric_id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT NOT NULL,
        timestamp INTEGER NOT NULL,
        rsi REAL,
        macd REAL,
        adx REAL,
        ema20 REAL,
        ema50 REAL,
        atr REAL
    );

    CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades(timestamp);
    CREATE INDEX IF NOT EXISTS idx_signals_timestamp ON signals(timestamp);
    CREATE INDEX IF NOT EXISTS idx_metrics_timestamp ON metrics(timestamp);
    """
    try:
        with get_db_connection() as conn:
            logger.info("[DB] Executing schema creation")
            conn.executescript(schema)
            conn.commit()
            logger.info("Tables créées ou vérifiées avec succès.")
    except Exception as e:
        logger.error(f"Erreur création tables: {e}")
        raise

# Insertion d'un trade
def insert_trade(order_details):
    required = ["symbol", "side", "quantity", "price", "timestamp"]
    if not all(k in order_details for k in required):
        logger.error(f"Données manquantes dans order_details: {order_details}")
        return

    with get_db_connection() as conn:
        cur = conn.cursor()
        try:
            logger.debug(f"Tentative d'insertion trade: {order_details}")
            cur.execute("""
                INSERT INTO trades (order_id, symbol, side, quantity, price, stop_loss, take_profit, timestamp, pnl)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                order_details.get("order_id"),
                order_details["symbol"],
                order_details["side"],
                float(order_details["quantity"]),
                float(order_details["price"]),
                float(order_details.get("stop_loss", 0)),
                float(order_details.get("take_profit", 0)),
                int(order_details["timestamp"]),
                float(order_details.get("pnl", 0)),
            ))
            conn.commit()
            logger.info(f"Trade inséré: {order_details}")
        except Exception as e:
            logger.error(f"Erreur insertion trade: {e}")
            raise
        finally:
            cur.close()

# Insertion d'un signal
def insert_signal(signal_details):
    required = ["symbol", "signal_type", "price", "timestamp"]
    if not all(k in signal_details for k in required):
        logger.error(f"Données manquantes dans signal_details: {signal_details}")
        return

    valid_signals = {'buy', 'sell', 'close_buy', 'close_sell'}
    signal_type = signal_details.get("signal_type")

    if signal_type not in valid_signals:
        logger.error(f"signal_type invalide: {signal_type} - skipping insertion")
        return

    with get_db_connection() as conn:
        cur = conn.cursor()
        try:
            logger.debug(f"Tentative d'insertion signal: {signal_details}")
            cur.execute("""
                INSERT INTO signals (symbol, signal_type, price, quantity, timestamp)
                VALUES (?, ?, ?, ?, ?)
            """, (
                signal_details["symbol"],
                signal_type,
                float(signal_details["price"]),
                float(signal_details.get("quantity", 0)),
                int(signal_details["timestamp"]),
            ))
            conn.commit()
            logger.info(f"Signal inséré: {signal_details}")
        except Exception as e:
            logger.error(f"Erreur insertion signal: {e}")
            raise
        finally:
            cur.close()

# Insertion des métriques
def insert_metrics(metric_details):
    required = ["symbol", "timestamp"]
    if not all(k in metric_details for k in required):
        logger.error(f"Données manquantes dans metric_details: {metric_details}")
        return

    # Validation RSI
    if not (0 <= metric_details.get("rsi", 0) <= 100):
        logger.error(f"Invalid RSI: {metric_details['rsi']}")
        return

    with get_db_connection() as conn:
        cur = conn.cursor()
        try:
            # Prevent duplicate metrics for the same symbol and timestamp
            cur.execute("SELECT 1 FROM metrics WHERE symbol = ? AND timestamp = ?", 
                        (metric_details["symbol"], metric_details["timestamp"]))
            if cur.fetchone():
                return

            logger.debug(f"Tentative d'insertion métriques: {metric_details}")
            cur.execute("""
                INSERT INTO metrics (symbol, timestamp, rsi, macd, adx, ema20, ema50, atr)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                metric_details["symbol"],
                int(metric_details["timestamp"]),
                float(metric_details.get("rsi", 0)),
                float(metric_details.get("macd", 0)),
                float(metric_details.get("adx", 0)),
                float(metric_details.get("ema20", 0)),
                float(metric_details.get("ema50", 0)),
                float(metric_details.get("atr", 0)),
            ))
            conn.commit()
            logger.info(f"Métriques insérées: {metric_details}")
        except Exception as e:
            logger.error(f"Erreur insertion métriques: {e}")
            raise
        finally:
            cur.close()

# Récupération des données
def get_trades():
    with get_db_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute("SELECT * FROM trades ORDER BY timestamp DESC")
            return cur.fetchall()
        except Exception as e:
            logger.error(f"Erreur récupération trades: {e}")
            return []
        finally:
            cur.close()

def get_signals():
    with get_db_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute("SELECT * FROM signals ORDER BY timestamp DESC")
            return cur.fetchall()
        except Exception as e:
            logger.error(f"Erreur récupération signals: {e}")
            return []
        finally:
            cur.close()

def get_metrics():
    with get_db_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute("SELECT * FROM metrics ORDER BY timestamp DESC")
            return cur.fetchall()
        except Exception as e:
            logger.error(f"Erreur récupération métriques: {e}")
            return []
        finally:
            cur.close()

# Point d'entrée pour tests
if __name__ == "__main__":
    create_tables()