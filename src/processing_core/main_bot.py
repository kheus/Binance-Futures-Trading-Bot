import eventlet
from rich import inspect
eventlet.monkey_patch()
import asyncio
import yaml
import pandas as pd
import talib
import logging
from logging.handlers import RotatingFileHandler
from binance.um_futures import UMFutures
from confluent_kafka import Consumer
from src.data_ingestion.data_formatter import format_candle
from src.processing_core.lstm_model import train_or_load_model
from src.processing_core.signal_generator import check_signal, prepare_lstm_input
from src.trade_execution.order_manager import place_order, update_trailing_stop, init_trailing_stop_manager
from src.database.db_handler import insert_trade, insert_signal, insert_metrics, create_tables, execute_query
from src.monitoring.metrics import record_trade_metric
from monitoring.alerting import send_telegram_alert
import platform
import sys
import os
import time
import numpy as np
import threading
from src.performance.tracker import performance_tracker_loop
from tabulate import tabulate
import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8-sig")

import builtins, traceback

orig_open = builtins.open

def tracked_open(file, mode='r', *args, **kwargs):
    try:
        return orig_open(file, mode, *args, **kwargs)
    except Exception as e:
        print(f"[OPEN ERROR] fichier: {file} mode: {mode} -> {e}")
        traceback.print_exc()
        raise

builtins.open = tracked_open

# Force UTF-8 encoding for the console
sys.stdout.reconfigure(encoding="utf-8-sig")
sys.stderr.reconfigure(encoding="utf-8-sig")

# Set up logging
os.makedirs("logs", exist_ok=True)
log_file = "logs/trading_bot.log"
file_handler = RotatingFileHandler(log_file, maxBytes=1_000_000, backupCount=3)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(name)s] %(message)s',
    handlers=[
        logging.FileHandler('logs/trading_bot.log', encoding="utf-8-sig"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load configuration
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
config_path = os.path.join(project_root, 'config', 'config.yaml')
kafka_config_path = os.path.join(project_root, 'config', 'kafka_config_local.yaml')

# Validate configuration files
for config_file in [config_path, kafka_config_path]:
    if not os.path.exists(config_file):
        logger.error(f"[Config Error] Configuration file not found: {config_file}")
        raise FileNotFoundError(f"Configuration file not found: {config_file}")

try:
    with open(config_path, 'r', encoding='utf-8-sig') as f:
        config = yaml.safe_load(f)
    with open(kafka_config_path, 'r', encoding='utf-8-sig') as f:
        kafka_config = yaml.safe_load(f)
except Exception as e:
    logger.error(f"[Config Error] Failed to load configuration files: {e}")
    raise Exception(f"Failed to load configuration files: {e}")

# Validate required configuration keys
required_top_level_keys = ["binance"]
for key in required_top_level_keys:
    if key not in config or not config[key]:
        logger.error(f"[Config Error] Missing or invalid key in config.yaml: {key} - Config: {config}")
        raise ValueError(f"Missing or invalid key in config.yaml: {key}")

# Validate binance section structure
if not isinstance(config["binance"], dict):
    logger.error("[Config Error] 'binance' must be a dictionary in config.yaml")
    raise ValueError("'binance' must be a dictionary in config.yaml")

binance_config = config["binance"]
required_binance_keys = ["symbols", "timeframe", "capital", "leverage"]
for key in required_binance_keys:
    if key not in binance_config or not binance_config[key]:
        logger.error(f"[Config Error] Missing or invalid '{key}' under 'binance' in config.yaml - Config: {binance_config}")
        raise ValueError(f"Missing or invalid '{key}' under 'binance' in config.yaml")
    if key == "symbols":
        if not isinstance(binance_config["symbols"], list) or not binance_config["symbols"]:
            logger.error("[Config Error] 'symbols' must be a non-empty list under 'binance' in config.yaml")
            raise ValueError("'symbols' must be a non-empty list under 'binance' in config.yaml")
        if not all(isinstance(s, str) for s in binance_config["symbols"]):
            logger.error("[Config Error] All 'symbols' must be strings under 'binance' in config.yaml")
            raise ValueError("All 'symbols' must be strings under 'binance' in config.yaml")
    elif key == "timeframe" and not isinstance(binance_config["timeframe"], str):
        logger.error("[Config Error] 'timeframe' must be a string under 'binance' in config.yaml")
        raise ValueError("'timeframe' must be a string under 'binance' in config.yaml")
    elif key in ["capital", "leverage"] and not isinstance(binance_config[key], (int, float)):
        logger.error(f"[Config Error] '{key}' must be a number under 'binance' in config.yaml")
        raise ValueError(f"'{key}' must be a number under 'binance' in config.yaml")

if "kafka" not in kafka_config or "bootstrap_servers" not in kafka_config["kafka"]:
    logger.error("[Config Error] Missing or invalid Kafka configuration in kafka_config_local.yaml")
    raise ValueError("Missing or invalid Kafka configuration in kafka_config_local.yaml")

SYMBOLS = binance_config["symbols"]
TIMEFRAME = binance_config["timeframe"]
CAPITAL = binance_config["capital"]
LEVERAGE = binance_config["leverage"]
KAFKA_BOOTSTRAP = kafka_config["kafka"]["bootstrap_servers"]
MODEL_UPDATE_INTERVAL = 900  # Model retraining every 15 minutes

# Initialize Binance client
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
logger.info(f"[Debug] sys.path: {sys.path}")
try:
    from src.trade_execution.binance_client import init_binance_client
    logger.info("[Debug] binance_client module imported successfully")
except ImportError as e:
    logger.error(f"[Import Error] Failed to import binance_client: {e}")
    raise Exception("Binance client module not found")
client = UMFutures(key="f52c3046240c9514626cf7619ea6bb93f329e2ad39dac256291f48655e750545", secret="20fe2ccf55a7114e576e5830e6ebadbdfdb66df849a326d50ebfb2ca394ce7ec", base_url="https://testnet.binancefuture.com")
print(client.account())
if client is None:
    logger.error("[Binance Client] Failed to initialize, exiting.")
    raise Exception("Binance client initialization failed")

from src.trade_execution.sync_orders import sync_binance_orders_with_postgres

# ⚙️ Synchronisation initiale des ordres Binance ↔ PostgreSQL
logger.info("[MainBot] Syncing Binance open orders with PostgreSQL and internal tracker...")
sync_binance_orders_with_postgres(client, SYMBOLS)
logger.info("[MainBot] Sync completed. Proceeding with normal bot operation...")

def periodic_sync(client, symbols, interval=300):  # toutes les 5 minutes
    while True:
        logger.info("[Sync] Starting periodic Binance ↔ PostgreSQL check")
        sync_binance_orders_with_postgres(client, symbols)
        time.sleep(interval)

# Démarrer la synchronisation périodique dans un thread dédié
sync_thread = threading.Thread(target=periodic_sync, args=(client, SYMBOLS))
sync_thread.daemon = True
sync_thread.start()

# Initialize trailing stop manager
ts_manager = init_trailing_stop_manager(client)

# Initialize Kafka consumer
try:
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "trading_bot_group",
        "auto.offset.reset": "latest",
        "security.protocol": "PLAINTEXT"
    })
    logger.info(f"[Kafka] Consumer initialized with bootstrap servers: {KAFKA_BOOTSTRAP}")
except Exception as e:
    logger.error(f"[Kafka] Failed to initialize consumer: {e}")
    raise Exception("Kafka consumer initialization failed")

inserted_metrics = []  # À placer en haut du fichier ou dans une portée globale si besoin

def calculate_indicators(df, symbol):
    logger.info(f"Calculating indicators for {symbol} DataFrame with {len(df)} rows")
    if len(df) < 50:
        logger.info(f"Skipping indicator calculation for {symbol} due to insufficient data")
        return df

    try:
        df['RSI'] = talib.RSI(df['close'], timeperiod=14)
        df['MACD'], df['MACD_signal'], df['MACD_hist'] = talib.MACD(df['close'], fastperiod=12, slowperiod=26, signalperiod=9)
        df['ADX'] = talib.ADX(df['high'], df['low'], df['close'], timeperiod=14)
        df['EMA20'] = talib.EMA(df['close'], timeperiod=20)
        df['EMA50'] = talib.EMA(df['close'], timeperiod=50)
        df['ATR'] = talib.ATR(df['high'], df['low'], df['close'], timeperiod=14)

        required_cols = ['RSI', 'MACD', 'MACD_signal', 'MACD_hist', 'ADX', 'EMA20', 'EMA50', 'ATR']
        df[required_cols] = df[required_cols].ffill().bfill().interpolate()

        try:
            last_row = df[required_cols].iloc[-1].to_dict()
            logger.info(f"[Debug] Indicators after update for {symbol}: {last_row}")
        except Exception as e:
            logger.warning(f"[Indicators] Failed to extract debug indicators for {symbol}: {e}")

        logger.info(f"After filling NaN, {symbol} DataFrame has {len(df)} rows")

        metrics = {
            "symbol": symbol,
            "timestamp": int(df.index[-1].timestamp() * 1000),
            "rsi": float(df['RSI'].iloc[-1]),
            "macd": float(df['MACD'].iloc[-1]),
            "adx": float(df['ADX'].iloc[-1]),
            "ema20": float(df['EMA20'].iloc[-1]),
            "ema50": float(df['EMA50'].iloc[-1]),
            "atr": float(df['ATR'].iloc[-1])
        }

        if not hasattr(calculate_indicators, "last_metrics"):
            calculate_indicators.last_metrics = []

        if not any(m["timestamp"] == metrics["timestamp"] for m in calculate_indicators.last_metrics):
            insert_metrics(symbol, metrics)
            calculate_indicators.last_metrics.append(metrics)
            logger.info(f"[Metrics] Inserted metrics for {symbol}: {metrics}")

    except Exception as e:
        logger.error(f"[Indicators] Error calculating indicators for {symbol}: {e}")
        import traceback
        logger.error(traceback.format_exc())

    return df
def insert_price_data(candle_df, symbol):
    try:
        latest = candle_df.iloc[-1]
        timestamp_ms = int(latest.name.timestamp() * 1000)
        query = """
            INSERT INTO price_data (symbol, timestamp, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, timestamp) DO UPDATE
            SET open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume
        """
        params = (
            symbol,
            timestamp_ms,
            float(latest['open']),
            float(latest['high']),
            float(latest['low']),
            float(latest['close']),
            float(latest['volume'])
        )
        execute_query(query, params)
        logger.info(f"[Price Data] Inserted price data for {symbol}: {params}")
    except Exception as e:
        logger.error(f"[Price Data] Error inserting price data for {symbol}: {e}")

def log_indicators_as_table(symbol, indicators, timestamp):
    table_data = [
        ["Timestamp", timestamp],
        ["Symbol", symbol],
        ["RSI", f"{indicators['RSI']:.2f}"],
        ["MACD", f"{indicators['MACD']:.2f}"],
        ["MACD_signal", f"{indicators['MACD_signal']:.2f}"],
        ["MACD_hist", f"{indicators['MACD_hist']:.2f}"],
        ["ADX", f"{indicators['ADX']:.2f}"],
        ["EMA20", f"{indicators['EMA20']:.2f}"],
        ["EMA50", f"{indicators['EMA50']:.2f}"],
        ["ATR", f"{indicators['ATR']:.2f}"]
    ]
    logger.info("Indicators Table:\n%s", tabulate(table_data, headers=["Metric", "Value"], tablefmt="grid"))

async def main():
    dataframes = {symbol: pd.DataFrame() for symbol in SYMBOLS}
    order_details = {symbol: None for symbol in SYMBOLS}
    current_positions = {symbol: None for symbol in SYMBOLS}
    last_order_details = {symbol: None for symbol in SYMBOLS}
    last_model_updates = {symbol: time.time() for symbol in SYMBOLS}
    last_action_sent = {symbol: None for symbol in SYMBOLS}
    last_sl_order_ids = {symbol: None for symbol in SYMBOLS}
    models = {symbol: None for symbol in SYMBOLS}

    try:
        logger.info("[Main] Creating database tables")
        schema_path = os.path.join(project_root, 'src', 'database', 'schema.sql')
        if not os.path.exists(schema_path):
            logger.error(f"[Config Error] Database schema file not found: {schema_path}")
            raise FileNotFoundError(f"Database schema file not found: {schema_path}")
        with open(schema_path, "r", encoding="utf-8-sig") as file:
            schema_content = file.read()
        create_tables()
        logger.info("[Main] Database tables created successfully")
    except Exception as e:
        logger.error(f"[Main] Error creating database tables: {e}")
        raise

    try:
        client.time()
        logger.info("[Binance Client] API connectivity confirmed")
    except Exception as e:
        logger.error(f"[API Connectivity] Failed: {e}")
        raise Exception("API connectivity test failed")

    for symbol in SYMBOLS:
        logger.info(f"[Main] Fetching historical data for {symbol} with timeframe {TIMEFRAME}")
        klines = []
        limit = 1500
        start_time = None
        while len(klines) < 2000:
            try:
                new_klines = client.klines(symbol=symbol, interval=TIMEFRAME, limit=limit, startTime=start_time)
                if not new_klines or len(new_klines) == 0:
                    break
                klines.extend(new_klines)
                start_time = int(new_klines[-1][0]) + 1
                logger.info(f"[Data Fetch] Fetched {len(new_klines)} klines for {symbol}, total: {len(klines)}")
            except Exception as e:
                logger.error(f"[Data Fetch] Failed for {symbol} with limit={limit}: {e}")
                limit = max(500, limit - 100)
                if limit < 500:
                    raise ValueError(f"Unable to fetch sufficient historical data for {symbol}")
        if len(klines) < 101:
            logger.error(f"[Data] Insufficient historical data fetched for {symbol}: {len(klines)} rows")
            raise ValueError(f"Insufficient historical data for {symbol}")
        data_hist = pd.DataFrame(klines, columns=["open_time", "open", "high", "low", "close", "volume",
                                                "close_time", "quote_asset_vol", "num_trades", "taker_buy_base_vol",
                                                "taker_buy_quote_vol", "ignore"])
        data_hist = data_hist[["open_time", "open", "high", "low", "close", "volume"]].astype(float)
        dataframes[symbol] = data_hist.rename(columns={"open_time": "timestamp"})
        dataframes[symbol]["timestamp"] = pd.to_datetime(dataframes[symbol]["timestamp"], unit="ms")
        dataframes[symbol].set_index("timestamp", inplace=True)
        dataframes[symbol] = dataframes[symbol].tail(200)
        dataframes[symbol] = calculate_indicators(dataframes[symbol], symbol)

        logger.info(f"[Main] Training or loading LSTM model for {symbol}")
        models[symbol] = train_or_load_model(dataframes[symbol])
        if models[symbol] is None:
            logger.warning(f"[Model] Using mock model for {symbol} due to failure")
            class MockModel:
                def predict(self, x, verbose=0):
                    return np.array([[0.5]])
            models[symbol] = MockModel()

    topics = [f"{symbol}_candle" for symbol in SYMBOLS]
    try:
        consumer.subscribe(topics)
        logger.info(f"[Kafka] Subscribed to topics: {topics}")
    except Exception as e:
        logger.error(f"[Kafka] Failed to subscribe to topics: {e}")
        raise

    last_log_time = time.time()
    iteration_count = 0
    try:
        while True:
            if iteration_count % 10 == 0:
                logger.info("[Main Loop] Starting iteration")
            iteration_count += 1
            msg = consumer.poll(1.0)
            current_time = time.time()

            if msg is None:
                if current_time - last_log_time >= 5:
                    logger.info("[Main Loop] Running, awaiting data")
                    last_log_time = current_time
                for symbol in SYMBOLS:
                    if current_time - last_model_updates[symbol] >= MODEL_UPDATE_INTERVAL and len(dataframes[symbol]) >= 101:
                        logger.info(f"[Main] Updating LSTM model for {symbol} with new data")
                        models[symbol] = train_or_load_model(dataframes[symbol])
                        if models[symbol] is None:
                            logger.error(f"[Model] No valid model available for {symbol}, using mock prediction")
                            class MockModel:
                                def predict(self, x, verbose=0):
                                    return np.array([[0.5]])
                            models[symbol] = MockModel()
                        last_model_updates[symbol] = current_time
                        send_telegram_alert(f"Model updated successfully for {symbol}.")
                        send_telegram_alert(f"Bingo ! Model updated successfully for {symbol}.")
                        lstm_input = prepare_lstm_input(dataframes[symbol])
                        pred = models[symbol].predict(lstm_input, verbose=0)[0][0]
                        logger.info(f"[Look] Prediction after update: {pred}")
                        send_telegram_alert(f"[Look] Prediction after update: {pred}")

                await asyncio.sleep(0.1)
                continue

            if msg.error():
                logger.error(f"[Kafka] Consumer error: {msg.error()}")
                continue

            try:
                candle_data = msg.value().decode("utf-8") if msg.value() else None
                if not candle_data or not isinstance(candle_data, str):
                    logger.error(f"[Kafka] Invalid candle data for topic {msg.topic()}: {candle_data}")
                    continue
                topic = msg.topic()
                symbol = next((s for s in SYMBOLS if f"{s}_candle" in topic), None)
                if not symbol or symbol not in dataframes:
                    logger.error(f"[Kafka] Invalid symbol for topic {topic}")
                    continue

                candle_df = format_candle(candle_data)
                if candle_df.empty or "close" not in candle_df.columns:
                    logger.error(f"[Kafka] Invalid or empty candle data for {symbol}: {candle_data}")
                    continue
                candle_df["timestamp"] = pd.to_datetime(candle_df["timestamp"], unit="ms")
                candle_df.set_index("timestamp", inplace=True)
                insert_price_data(candle_df, symbol)
                dataframes[symbol] = pd.concat([dataframes[symbol], candle_df], ignore_index=False)
                dataframes[symbol] = calculate_indicators(dataframes[symbol], symbol)
                if len(dataframes[symbol]) >= 101:
                    action, new_position = check_signal(
                        dataframes[symbol],
                        models[symbol],
                        current_positions[symbol],
                        last_order_details[symbol],
                        symbol
                    )
                    if action == last_action_sent.get(symbol):
                        logger.info(f"[Signal] Ignored repeated action for {symbol}: {action}")
                        continue
                    last_action_sent[symbol] = action

                    if action in ["buy", "sell", "close_buy", "close_sell"]:
                        signal_details = {
                            "symbol": symbol,
                            "signal_type": action,
                            "price": float(candle_df["close"].iloc[-1]),
                            "timestamp": int(candle_df.index[-1].timestamp() * 1000)
                        }
                        # Évite les doublons d'insertion de signaux
                        if not any(s["timestamp"] == signal_details["timestamp"] for s in getattr(main, "last_signals", [])):
                            insert_signal(signal_details)
                            if not hasattr(main, "last_signals"):
                                main.last_signals = []
                            main.last_signals.append(signal_details)
                        logger.info(f"[Signal] Stored signal details for {symbol}: {signal_details}")

                        if action in ["buy", "sell"]:
                            try:
                                price = float(candle_df["close"].iloc[-1])
                                atr = float(dataframes[symbol]["ATR"].iloc[-1]) if 'ATR' in dataframes[symbol].columns and not np.isnan(dataframes[symbol]["ATR"].iloc[-1]) else 0
                                order_details[symbol] = place_order(action, price, atr, client, symbol, CAPITAL, LEVERAGE)
                                if order_details[symbol]:
                                    insert_trade(order_details[symbol])
                                    last_order_details[symbol] = order_details[symbol]
                                    record_trade_metric(order_details[symbol])
                                    send_telegram_alert(f"Trade executed: {action.upper()} {symbol} at {price}")
                                    current_positions[symbol] = new_position
                                    current_market_price = float(candle_df["close"].iloc[-1])
                                    last_sl_order_ids[symbol] = update_trailing_stop(
                                        client=client,
                                        symbol=symbol,
                                        signal=action,
                                        current_price=current_market_price,
                                        atr=atr,
                                        base_qty=float(order_details[symbol]["quantity"]),
                                        existing_sl_order_id=None
                                    )
                                else:
                                    logger.error(f"[Order] Failed to place {action} order for {symbol}")
                            except (TypeError, ValueError, IndexError) as e:
                                logger.error(f"[Order] Error placing {action} order for {symbol}: {e}")
                        elif action in ["close_buy", "close_sell"]:
                            try:
                                close_side = "sell" if action == "close_buy" else "buy"
                                price = float(candle_df["close"].iloc[-1])
                                atr = float(dataframes[symbol]["ATR"].iloc[-1]) if 'ATR' in dataframes[symbol].columns and not np.isnan(dataframes[symbol]["ATR"].iloc[-1]) else 0
                                order_details[symbol] = place_order(close_side, price, atr, client, symbol, CAPITAL, LEVERAGE)
                                if order_details[symbol]:
                                    if last_order_details[symbol] and last_order_details[symbol].get("price"):
                                        open_price = float(last_order_details[symbol]["price"])
                                        close_price = float(order_details[symbol]["price"])
                                        side = last_order_details[symbol]["side"]
                                        qty = float(last_order_details[symbol]["quantity"])
                                        pnl = (close_price - open_price) * qty if side == "buy" else (open_price - close_price) * qty
                                        order_details[symbol]["pnl"] = pnl
                                    else:
                                        order_details[symbol]["pnl"] = 0.0
                                    insert_trade(order_details[symbol])
                                    record_trade_metric(order_details[symbol])
                                    send_telegram_alert(f"Trade Closed: {action.upper()} {symbol} at {price}\nPNL: {order_details[symbol]['pnl']:.2f} USDT")
                                    last_order_details[symbol] = None
                                    current_positions[symbol] = None
                                else:
                                    logger.error(f"[Order] Failed to close {action} for {symbol}")
                            except (TypeError, ValueError, IndexError) as e:
                                logger.error(f"[Order] Error closing {action} for {symbol}: {e}")

                        current_market_price = float(candle_df["close"].iloc[-1])
                        current_atr = float(dataframes[symbol]["ATR"].iloc[-1]) if 'ATR' in dataframes[symbol].columns and not np.isnan(dataframes[symbol]["ATR"].iloc[-1]) else 0
                        if current_positions[symbol] is not None and last_sl_order_ids[symbol] is not None:
                            try:
                                last_sl_order_ids[symbol] = update_trailing_stop(
                                    client=client,
                                    symbol=symbol,
                                    signal=action,
                                    current_price=current_market_price,
                                    atr=current_atr,
                                    base_qty=last_order_details[symbol]["quantity"] if last_order_details[symbol] else 0,
                                    existing_sl_order_id=last_sl_order_ids[symbol]
                                )
                            except (TypeError, ValueError) as e:
                                logger.error(f"[Trailing Stop] Error updating for {symbol}: {e}")
                        dataframes[symbol] = dataframes[symbol].tail(200)
            except Exception as e:
                logger.error(f"[Kafka] Exception while processing candle for {symbol}: {e}")
                continue
            await asyncio.sleep(0.1)

    except Exception as e:
        logger.error(f"[Main] Bot error: {e}")
        send_telegram_alert(f"Bot error: {str(e)}")
    finally:
        logger.info("[Main] Closing Kafka consumer")
        consumer.close()

if platform.system() == "Emscripten":
    asyncio.ensure_future(main())
else:
    if __name__ == "__main__":
        tracker_thread = threading.Thread(target=performance_tracker_loop, daemon=True)
        tracker_thread.start()
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            logger.info("Script terminated by user")
        except Exception as e:
            logger.error(f"[Main] Unexpected error in main loop: {e}")
            send_telegram_alert(f"Unexpected error: {str(e)}")

