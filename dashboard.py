import logging
from logging.handlers import RotatingFileHandler
import os
import yaml
import pandas as pd
from pathlib import Path
from flask import Flask, render_template, request
from flask_socketio import SocketIO, emit
import psycopg2
import threading
import gc
import time
from decimal import Decimal
import socket
from contextlib import closing
from datetime import datetime

# === Chargement config ===
BASE_DIR = Path(__file__).parent.parent
config_path = "C:/Users/Cheikh/binance-trading-bot/config/db_config.yaml"

try:
    with open(config_path, 'r', encoding='utf-8-sig') as f:
        config = yaml.safe_load(f)
except Exception as e:
    print(f"Error loading config: {e}")
    config = {
        "database": {
            "type": "sqlite",
            "sqlite": {"path": str(BASE_DIR / "trading_bot.db")}
        },
        "trading": {
            "symbols": ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"]
        }
    }

# === Logging ===
os.makedirs("logs", exist_ok=True)
log_file = "logs/dashboard.log"
file_handler = RotatingFileHandler(log_file, maxBytes=1_000_000, backupCount=3)
file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

logging.basicConfig(
    encoding="utf-8-sig",
    level=logging.INFO,
    handlers=[file_handler, console_handler]
)

logger = logging.getLogger(__name__)
logging.getLogger('socketio').setLevel(logging.CRITICAL)
logging.getLogger('werkzeug').setLevel(logging.WARNING)
logging.getLogger('engineio').setLevel(logging.CRITICAL)

# === Flask/SocketIO ===
app = Flask(__name__, template_folder='templates')
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")

# === Constantes ===
SYMBOLS = config["trading"].get("symbols", ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"])
logger.info(f"Using symbols: {SYMBOLS}")

def convert_decimal_to_float(data):
    if isinstance(data, dict):
        return {k: convert_decimal_to_float(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [convert_decimal_to_float(item) for item in data]
    elif isinstance(data, Decimal):
        return float(data)
    elif isinstance(data, datetime):
        return int(data.timestamp() * 1000)  # Convert to milliseconds
    return data

def get_trades():
    try:
        conn = psycopg2.connect(**config["database"]["postgresql"])
        cursor = conn.cursor()
        cursor.execute("""
            SELECT order_id, order_id AS trade_id, symbol, side, quantity, price, exit_price, status, (timestamp::double precision * 1000)::bigint AS timestamp, realized_pnl
            FROM trades ORDER BY timestamp DESC LIMIT 20
        """)
        trades = cursor.fetchall()
        conn.close()
        return trades
    except Exception as e:
        logger.error(f"Error fetching trades: {e}", exc_info=True)
        return []

def get_signals():
    try:
        conn = psycopg2.connect(**config["database"]["postgresql"])
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id AS signal_id, symbol, signal_type, price, quantity, strategy, timestamp, confidence_score, created_at
            FROM signals ORDER BY timestamp DESC LIMIT 20
        """)
        signals_df = pd.DataFrame(
            cursor.fetchall(),
            columns=["signal_id", "symbol", "signal_type", "price", "quantity", "strategy", "timestamp", "confidence_score", "created_at"]
        )
        signals_df = signals_df.drop_duplicates(subset=["signal_id", "timestamp"], inplace=False)
        conn.close()
        return signals_df
    except Exception as e:
        logger.error(f"Error fetching signals: {e}", exc_info=True)
        return pd.DataFrame()

def get_latest_data():
    data = {
        "prices": [],
        "trades": [],
        "signals": [],
        "metrics": {},
        "training_count": 0
    }
    
    # Prix
    try:
        conn = psycopg2.connect(**config["database"]["postgresql"])
        cursor = conn.cursor()
        for symbol in SYMBOLS:
            cursor.execute(
                "SELECT timestamp, close FROM price_data WHERE symbol = %s ORDER BY timestamp DESC LIMIT 1",
                (symbol,)
            )
            result = cursor.fetchone()
            if result:
                timestamp, close = result
                data["prices"].append({"symbol": symbol, "price": close, "timestamp": timestamp})
        conn.close()
    except Exception as e:
        logger.error(f"Error fetching prices: {e}", exc_info=True)

    # Trades
    try:
        conn = psycopg2.connect(**config["database"]["postgresql"])
        cursor = conn.cursor()
        cursor.execute("""
            SELECT order_id, order_id AS trade_id, symbol, side, quantity, price, exit_price, status, timestamp, realized_pnl
            FROM trades ORDER BY timestamp DESC LIMIT 20
        """)
        trade_rows = cursor.fetchall()
        if trade_rows:
            columns = ["order_id", "trade_id", "symbol", "side", "quantity", "price", "exit_price", "status", "timestamp", "realized_pnl"]
            df = pd.DataFrame(trade_rows, columns=columns).drop_duplicates()
            logger.info(f"Last 5 trades: {df.tail(5).to_dict(orient='records')}")
            data["trades"] = df.tail(5).to_dict(orient='records')
        conn.close()
    except Exception as e:
        logger.error(f"Error processing trades: {e}", exc_info=True)

    # Signaux
    try:
        conn = psycopg2.connect(**config["database"]["postgresql"])
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id AS signal_id, symbol, signal_type, price, strategy, timestamp, confidence_score, created_at
            FROM signals ORDER BY timestamp DESC LIMIT 20
        """)
        signals_df = pd.DataFrame(
            cursor.fetchall(),
            columns=["signal_id", "symbol", "signal_type", "price", "strategy", "timestamp", "confidence_score", "created_at"]
        )
        if not signals_df.empty:
            logger.info(f"Last 5 signals: {signals_df.tail(5).to_dict(orient='records')}")
            data["signals"] = signals_df.tail(5).to_dict(orient='records')
        conn.close()
    except Exception as e:
        logger.error(f"Error processing signals: {e}", exc_info=True)

    # Indicateurs
    try:
        conn = psycopg2.connect(**config["database"]["postgresql"])
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id AS metric_id, symbol, timestamp, rsi, macd, adx, ema20, ema50, atr
            FROM metrics ORDER BY timestamp DESC LIMIT 100
        """)
        metrics = cursor.fetchall()
        columns = ["metric_id", "symbol", "timestamp", "rsi", "macd", "adx", "ema20", "ema50", "atr"]
        df = pd.DataFrame(metrics, columns=columns)
        for symbol in SYMBOLS:
            symbol_metrics = df[df["symbol"] == symbol]
            if not symbol_metrics.empty:
                latest = symbol_metrics.loc[symbol_metrics["timestamp"].idxmax()].to_dict()
                data["metrics"][symbol] = {
                    "rsi": latest.get("rsi", 0),
                    "macd": latest.get("macd", 0),
                    "adx": latest.get("adx", 0),
                    "ema20": latest.get("ema20", 0),
                    "ema50": latest.get("ema50", 0),
                    "atr": latest.get("atr", 0),
                    "timestamp": latest.get("timestamp", 0)
                }
        conn.close()
    except Exception as e:
        logger.error(f"Error processing metrics: {e}", exc_info=True)

    # Count training data
    try:
        conn = psycopg2.connect(**config["database"]["postgresql"])
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM training_data")
        data["training_count"] = cursor.fetchone()[0]
        conn.close()
    except Exception as e:
        logger.error(f"Error fetching training data count: {e}", exc_info=True)

    return convert_decimal_to_float(data)

def update_data():
    while True:
        try:
            data = get_latest_data()
            logger.info(f"Emitting update  prices: {len(data['prices'])}, trades: {len(data['trades'])}, signals: {len(data['signals'])}, metrics: {list(data['metrics'].keys())}")
            socketio.emit('update', data)
        except Exception as e:
            logger.error(f"Error in update_data: {e}", exc_info=True)
        socketio.sleep(5)
        gc.collect()

@app.route('/')
def index():
    logger.info("Rendering dashboard template")
    return render_template('index.html')

@socketio.on('connect')
def handle_connect():
    logger.info(f'Client connected: {request.sid}')
    data = get_latest_data()
    emit('initial_data', data)

@socketio.on('disconnect')
def handle_disconnect():
    logger.info(f'Client disconnected: {request.sid}')

def find_free_port(start_port=5000, end_port=5050):
    for port in range(start_port, end_port + 1):
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
            try:
                s.bind(('localhost', port))
                return port
            except OSError:
                continue
    raise OSError("No free ports available")

# === Lancement serveur ===
if __name__ == "__main__":
    try:
        conn = psycopg2.connect(**config["database"]["postgresql"])
        cursor = conn.cursor()
        cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'metrics'
        """)
        for col in cursor.fetchall():
            logger.info(f" - {col[0]} ({col[1]})")
        conn.close()
    except Exception as e:
        logger.error(f"Error inspecting metrics table: {e}")

    threading.Thread(target=update_data, daemon=True).start()
    port = find_free_port()
    logger.info(f"Starting Flask server with SocketIO on http://localhost:{port}")
    socketio.run(app, host='localhost', port=port, use_reloader=False)