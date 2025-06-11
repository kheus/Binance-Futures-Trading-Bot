import asyncio
import yaml
import pandas as pd
import talib
import logging
from logging.handlers import RotatingFileHandler
from confluent_kafka import Consumer
from binance.um_futures import UMFutures
from src.data_ingestion.data_formatter import format_candle
from src.processing_core.lstm_model import train_or_load_model
from src.processing_core.signal_generator import check_signal
from src.trade_execution.order_manager import place_order
from src.database.db_handler import insert_trade
from src.monitoring.metrics import record_trade_metric
from src.monitoring.alerting import send_telegram_alert
import platform
import sys
import os
import time
import numpy as np

# Force UTF-8 encoding for the console
sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

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
    handlers=[file_handler, console_handler],
    encoding='utf-8'
)

logger = logging.getLogger(__name__)

# Load configuration
with open("config/config.yaml", "r") as f:
    config = yaml.safe_load(f)
with open("config/kafka_config_local.yaml", "r") as f:
    kafka_config = yaml.safe_load(f)

SYMBOL = config["binance"]["symbol"]
TIMEFRAME = config["binance"]["timeframe"]
CAPITAL = config["binance"]["capital"]
LEVERAGE = config["binance"]["leverage"]
KAFKA_TOPIC = kafka_config["kafka"]["topic"]
KAFKA_BOOTSTRAP = kafka_config["kafka"]["bootstrap_servers"]
MODEL_UPDATE_INTERVAL = 300  # Ré-entraînement toutes les 1 minutes (en secondes)

# Initialize Binance client
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
logger.info(f"[Debug] sys.path: {sys.path}")
try:
    from src.trade_execution.binance_client import init_binance_client
    logger.info("[Debug] binance_client module imported successfully")
except ImportError as e:
    logger.error(f"[Import Error] Failed to import binance_client: {e}")
    raise Exception("Binance client module not found")
client = init_binance_client(mode="testnet")
if client is None:
    logger.error("[Binance Client] Failed to initialize, exiting.")
    raise Exception("Binance client initialization failed")

# Initialize Kafka consumer
consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "group.id": "trading_bot_group",
    "auto.offset.reset": "latest",
    "security.protocol": "PLAINTEXT"
})

def calculate_indicators(df):
    logger.info(f"Calculating indicators for DataFrame with {len(df)} rows")
    print(f"Calculating indicators for DataFrame with {len(df)} rows")
    if len(df) < 50:
        logger.info("Skipping indicator calculation due to insufficient data")
        print("Skipping indicator calculation due to insufficient data")
        return df
    # Calcul des indicateurs
    df['RSI'] = talib.RSI(df['close'], timeperiod=14)
    df['MACD'], df['MACD_signal'], df['MACD_hist'] = talib.MACD(df['close'], fastperiod=12, slowperiod=26, signalperiod=9)
    df['ADX'] = talib.ADX(df['high'], df['low'], df['close'], timeperiod=14)
    df['EMA20'] = df['close'].ewm(span=20, adjust=False).mean()
    df['EMA50'] = df['close'].ewm(span=50, adjust=False).mean()
    df['ATR'] = talib.ATR(df['high'], df['low'], df['close'], timeperiod=14)
    required_cols = ['RSI', 'MACD', 'MACD_signal', 'MACD_hist', 'ADX', 'EMA20', 'EMA50', 'ATR']
    # Remplissage robuste sur toutes les colonnes
    df[required_cols] = df[required_cols].ffill().bfill().fillna(0)
    logger.info(f"[Debug] Indicators after update: {df[required_cols].iloc[-1].to_dict()}")
    logger.info(f"After filling NaN, DataFrame has {len(df)} rows")
    print(f"After filling NaN, DataFrame has {len(df)} rows")
    return df

async def main():
    global client
    logger.info("[Main] Starting main coroutine")
    df = pd.DataFrame()
    order_details = None
    current_position = None
    last_order_details = None
    last_model_update = time.time()

    try:
        # Test client connectivity
        logger.info("[Main] Testing API connectivity")
        try:
            client.time()
            logger.info("[Binance Client] API connectivity confirmed")
        except Exception as e:
            logger.error(f"[API Connectivity] Failed: {e}")
            raise Exception("API connectivity test failed")

        # Load historical data
        logger.info(f"[Main] Fetching historical data for {SYMBOL} with timeframe {TIMEFRAME}")
        try:
            klines = client.klines(symbol=SYMBOL, interval=TIMEFRAME, limit=1500)
        except AttributeError:
            logger.warning("[Data Fetch] Falling back to alternative method")
            klines = client.get_klines(symbol=SYMBOL.upper(), interval=TIMEFRAME, startTime=None, endTime=None, limit=1500)
        logger.info(f"Fetched {len(klines)} klines, sample: {klines[0] if klines else 'None'}")
        if not klines or len(klines) < 51:
            logger.error(f"Insufficient historical data fetched: {len(klines)} rows")
            raise ValueError("Insufficient historical data")
        data_hist = pd.DataFrame(klines, columns=["open_time", "open", "high", "low", "close", "volume",
                                                 "close_time", "quote_asset_vol", "num_trades", "taker_buy_base_vol",
                                                 "taker_buy_quote_vol", "ignore"])
        data_hist = data_hist[["open_time", "open", "high", "low", "close", "volume"]].astype(float)
        df = data_hist.rename(columns={"open_time": "timestamp"})
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
        logger.info(f"Loaded {len(df)} historical candles")
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
        logger.info(f"[Data] Latest timestamp in df: {df['timestamp'].iloc[-1]}")

        # Compute indicators for historical data
        df = calculate_indicators(df)
        logger.info(f"DataFrame shape after indicators: {df.shape}, columns: {df.columns.tolist()}")

        # Train or load LSTM model
        logger.info("[Main] Training or loading LSTM model")
        model = train_or_load_model(df)
        if model is None:
            logger.warning("Using mock model due to failure")
            import numpy as np
            class MockModel:
                def predict(self, x, verbose=0):
                    return np.array([[0.6]])  # Example prediction
            model = MockModel()
        logger.info("Model loaded successfully")

        consumer.subscribe([KAFKA_TOPIC])
        last_log_time = time.time()
        while True:
            logger.info("[Main Loop] Starting iteration")
            msg = consumer.poll(1.0)
            current_time = time.time()

            # Gestion explicite de msg None
            if msg is None:
                if current_time - last_log_time >= 5:
                    logger.info("[Main Loop] Running, awaiting data")
                    last_log_time = current_time
                # Vérifier la mise à jour du modèle
                if current_time - last_model_update >= MODEL_UPDATE_INTERVAL and len(df) >= 60:
                    logger.info("[Main] Updating LSTM model with new data")
                    model = train_or_load_model(df)
                    if model is None:
                        logger.warning("Model update failed, using existing model")
                    else:
                        last_model_update = current_time
                        logger.info("Model updated successfully")
                        send_telegram_alert("🔄 Model updated successfully.")

                        from src.processing_core.signal_generator import prepare_lstm_input
                        lstm_input = prepare_lstm_input(df)
                        try:
                            pred = model.predict(lstm_input, verbose=0)[0][0]
                            logger.info(f"[Debug] Prediction after update: {pred}")
                            send_telegram_alert(f"[Debug] Prediction after update: {pred}")
                        except Exception as e:
                            logger.error(f"[Debug] Prediction failed: {e}")
                continue

            # Vérifier les erreurs uniquement si msg est un objet valide
            if hasattr(msg, 'error') and msg.error():
                logger.error(f"Kafka consumer error: {msg.error()}")
                print(f"Kafka consumer error: {msg.error()}")
                continue

            # Traitement du message si valide
            if hasattr(msg, 'value') and not msg.error():
                candle_df = format_candle(msg.value().decode("utf-8"))
                if not candle_df.empty:
                    logger.info(f"Received candle: {candle_df.iloc[-1]}")
                    print(f"Received candle: {candle_df.iloc[-1]}")
                    df = pd.concat([df, candle_df], ignore_index=True).tail(100)
                    df = calculate_indicators(df)
                    if len(df) >= 60:
                        action, new_position = check_signal(df, model, current_position, last_order_details)
                        logger.info(f"Action: {action}, New Position: {new_position}")
                        print(f"Action: {action}, New Position: {new_position}")
                        print(f"Current position before update: {current_position}")
                        if action in ["buy", "sell"]:
                            price = candle_df["close"].iloc[-1]
                            atr = df["ATR"].iloc[-1] if 'ATR' in df.columns else 0
                            logger.info(f"[Order Attempt] Action: {action}, Price: {price}, ATR: {atr}")
                            order_details = place_order(action, price, atr, client, SYMBOL, CAPITAL, LEVERAGE)
                            if order_details:
                                insert_trade(order_details)
                                last_order_details = order_details
                                record_trade_metric(order_details)
                                send_telegram_alert(f"Trade executed: {action.upper()} {SYMBOL} at {price}")
                                current_position = new_position
                            else:
                                logger.error(f"[Order Failed] No details returned for {action}")
                        elif action in ["close_buy", "close_sell"]:
                            close_side = "sell" if action == "close_buy" else "buy"
                            price = candle_df["close"].iloc[-1]
                            atr = df["ATR"].iloc[-1] if 'ATR' in df.columns else 0
                            logger.info(f"[Order Attempt] Action: {close_side}, Price: {price}, ATR: {atr}")
                            order_details = place_order(close_side, price, atr, client, SYMBOL, CAPITAL, LEVERAGE)
                            if order_details:
                                insert_trade(order_details)
                                record_trade_metric(order_details)
                                send_telegram_alert(f"Trade executed: {action.upper()} {SYMBOL} at {price}")
                                last_order_details = None
                                current_position = None
                        print(f"Current position after update: {current_position}")
            await asyncio.sleep(0.1)

    except Exception as e:
        logger.error(f"Main bot error: {e}")
        print(f"Main bot error: {e}")
        send_telegram_alert(f"Bot error: {str(e)}")
    finally:
        consumer.close()

if platform.system() == "Emscripten":
    asyncio.ensure_future(main())
else:
    if __name__ == "__main__":
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            logger.info("Script terminated by user")
        except Exception as e:
            logger.error(f"Unexpected error in main loop: {e}")
            print(f"Unexpected error: {e}")