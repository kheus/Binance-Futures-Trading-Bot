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
from datetime import datetime, timedelta

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
            "type": "postgresql",
            "postgresql": {}  # Add your PostgreSQL config here
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
        return data.isoformat()  # Return ISO format for frontend
    return data

def get_trades(symbol=None):
    try:
        conn = psycopg2.connect(**config["database"]["postgresql"])
        cursor = conn.cursor()
        query = """
            SELECT order_id, trade_id, symbol, side, quantity, price, exit_price, status, 
                   EXTRACT(EPOCH FROM timestamp) * 1000 AS timestamp, realized_pnl
            FROM trades
        """
        if symbol:
            query += " WHERE symbol = %s"
            cursor.execute(query + " ORDER BY timestamp DESC LIMIT 20", (symbol,))
        else:
            cursor.execute(query + " ORDER BY timestamp DESC LIMIT 20")
        trades = [
            {
                "order_id": row[0],
                "trade_id": row[1],
                "symbol": row[2],
                "side": row[3],
                "quantity": row[4],
                "price": row[5],
                "exit_price": row[6],
                "status": row[7],
                "timestamp": row[8],
                "realized_pnl": row[9]
            } for row in cursor.fetchall()
        ]
        conn.close()
        return trades
    except Exception as e:
        logger.error(f"Error fetching trades: {e}", exc_info=True)
        return []

def get_signals(symbol=None):
    try:
        conn = psycopg2.connect(**config["database"]["postgresql"])
        cursor = conn.cursor()
        query = """
            SELECT id AS signal_id, symbol, signal_type, price, quantity, strategy, timestamp, confidence_score, created_at
            FROM signals
        """
        if symbol:
            query += " WHERE symbol = %s"
            cursor.execute(query + " ORDER BY timestamp DESC LIMIT 20", (symbol,))
        else:
            cursor.execute(query + " ORDER BY timestamp DESC LIMIT 20")
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

def get_price_data(symbol, limit=30):
    try:
        conn = psycopg2.connect(**config["database"]["postgresql"])
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT timestamp, open, high, low, close, volume
            FROM price_data
            WHERE symbol = %s
            ORDER BY timestamp DESC
            LIMIT %s
            """,
            (symbol, limit)
        )
        price_data = cursor.fetchall()
        conn.close()
        return [
            {
                "timestamp": row[0],
                "open": row[1],
                "high": row[2],
                "low": row[3],
                "price": row[4],  # close price
                "volume": row[5]
            } for row in price_data
        ]
    except Exception as e:
        logger.error(f"Error fetching price data: {e}", exc_info=True)
        return []

def get_metrics_data(symbol, limit=30):
    try:
        conn = psycopg2.connect(**config["database"]["postgresql"])
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT timestamp, rsi, macd, adx, ema20, ema50, atr
            FROM metrics
            WHERE symbol = %s
            ORDER BY timestamp DESC
            LIMIT %s
            """,
            (symbol, limit)
        )
        metrics_data = cursor.fetchall()
        conn.close()
        return [
            {
                "timestamp": row[0],
                "rsi": row[1],
                "macd": row[2],
                "adx": row[3],
                "ema20": row[4],
                "ema50": row[5],
                "atr": row[6]
            } for row in metrics_data
        ]
    except Exception as e:
        logger.error(f"Error fetching metrics data: {e}", exc_info=True)
        return []

def get_latest_data(symbol=None):
    data = {
        "currentSignal": {},
        "priceInfo": {},
        "position": {},
        "indicators": {},
        "chartData": {},
        "signalHistory": [],
        "tradeHistory": []
    }

    # Current Signal
    signals_df = get_signals(symbol)
    if not signals_df.empty:
        latest_signal = signals_df.iloc[0].to_dict()
        data["currentSignal"] = {
            "action": latest_signal["signal_type"].lower(),
            "confidence": latest_signal["confidence_score"],
            "strategy": latest_signal["strategy"],
            "timestamp": latest_signal["created_at"]
        }

    # Price Info
    prices = get_price_data(symbol, 2) if symbol else []
    price_24h_ago = None
    if symbol:
        try:
            conn = psycopg2.connect(**config["database"]["postgresql"])
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT close
                FROM price_data
                WHERE symbol = %s AND timestamp <= %s
                ORDER BY timestamp DESC
                LIMIT 1
                """,
                (symbol, int((datetime.utcnow() - timedelta(hours=24)).timestamp() * 1000))
            )
            result = cursor.fetchone()
            price_24h_ago = result[0] if result else None
            conn.close()
        except Exception as e:
            logger.error(f"Error fetching 24h price: {e}", exc_info=True)

    data["priceInfo"] = {
        "current": prices[0]["price"] if prices else 0,
        "change": (prices[0]["price"] - price_24h_ago) if prices and price_24h_ago else 0,
        "changePercent": ((prices[0]["price"] - price_24h_ago) / price_24h_ago * 100) if prices and price_24h_ago else 0,
        "volume": prices[0]["volume"] if prices else 0,
        "atr": 0  # Will be updated from metrics
    }

    # Position
    trades = get_trades(symbol)
    if trades:
        latest_trade = pd.DataFrame(
            trades,
            columns=["order_id", "trade_id", "symbol", "side", "quantity", "price", "exit_price", "status", "timestamp", "realized_pnl"]
        ).iloc[0].to_dict()
        current_price = data["priceInfo"]["current"]
        data["position"] = {
            "type": latest_trade["side"].lower() if latest_trade["status"] == "open" else "none",
            "entryPrice": latest_trade["price"],
            "quantity": latest_trade["quantity"],
            "pnl": latest_trade["realized_pnl"] or (current_price - latest_trade["price"]) * latest_trade["quantity"] if current_price and latest_trade["status"] == "open" else 0,
            "pnlPercent": ((current_price - latest_trade["price"]) / latest_trade["price"] * 100) if current_price and latest_trade["price"] and latest_trade["status"] == "open" else 0
        }
    else:
        data["position"] = {
            "type": "none",
            "entryPrice": 0,
            "quantity": 0,
            "pnl": 0,
            "pnlPercent": 0
        }

    # Indicators
    metrics_data = get_metrics_data(symbol, 1) if symbol else []
    if metrics_data:
        latest_metrics = metrics_data[0]
        data["indicators"] = {
            "rsi": latest_metrics.get("rsi", 0),
            "macd": latest_metrics.get("macd", 0),
            "macdSignal": 0,  # Placeholder: Not in metrics table
            "macdHist": 0,    # Placeholder: Not in metrics table
            "adx": latest_metrics.get("adx", 0),
            "ema20": latest_metrics.get("ema20", 0),
            "ema50": latest_metrics.get("ema50", 0),
            "roc": 0,         # Placeholder: Not in metrics table
            "lstm": 0         # Placeholder: Not in metrics table
        }
        data["priceInfo"]["atr"] = latest_metrics.get("atr", 0)
    else:
        data["indicators"] = {
            "rsi": 0,
            "macd": 0,
            "macdSignal": 0,
            "macdHist": 0,
            "adx": 0,
            "ema20": 0,
            "ema50": 0,
            "roc": 0,
            "lstm": 0
        }

    # Chart Data
    price_data = get_price_data(symbol, 30) if symbol else []
    metrics_data = get_metrics_data(symbol, 30) if symbol else []
    data["chartData"] = {
        "labels": [datetime.fromtimestamp(p["timestamp"]/1000).strftime("%Y-%m-%d %H:%M:%S") for p in price_data],
        "prices": [p["price"] for p in price_data],
        "volumes": [p["volume"] for p in price_data],
        "rsi": [m["rsi"] or 0 for m in metrics_data],
        "macd": [m["macd"] or 0 for m in metrics_data],
        "macdSignal": [0 for _ in metrics_data],  # Placeholder
        "adx": [m["adx"] or 0 for m in metrics_data]
    }

    # Signal History
    data["signalHistory"] = signals_df.to_dict(orient="records") if not signals_df.empty else []

    # Trade History
    data["tradeHistory"] = get_trades(symbol) if symbol else []

    return convert_decimal_to_float(data)

def update_data():
    while True:
        try:
            for symbol in SYMBOLS:
                data = get_latest_data(symbol)
                logger.info(f"Emitting update for {symbol}: signals: {len(data['signalHistory'])}, trades: {len(data['tradeHistory'])}, metrics: {data['indicators']}")
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
    data = get_latest_data(SYMBOLS[0])  # Default to first symbol
    emit('initial_data', data)

@socketio.on('request_data')
def handle_request_data(data):
    symbol = data.get("symbol", SYMBOLS[0])
    logger.info(f"Received data request for symbol: {symbol}")
    try:
        emit('update', get_latest_data(symbol))
    except Exception as e:
        logger.error(f"Error processing data request for {symbol}: {e}", exc_info=True)
        emit('error', {"message": str(e)})

@socketio.on('close_position')
def handle_close_position(data):
    symbol = data.get("symbol", SYMBOLS[0])
    logger.info(f"Close position request for {symbol}")
    # Placeholder: Implement position closing logic with your trading bot
    emit('error', {"message": "Position closing not implemented"})

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