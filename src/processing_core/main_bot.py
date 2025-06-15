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
from src.processing_core.signal_generator import check_signal, prepare_lstm_input
from src.trade_execution.order_manager import place_order, update_trailing_stop
from src.database.db_handler import insert_trade, insert_signal, insert_metrics, create_tables
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
with open("config/config.yaml", "r", encoding='utf-8') as f:
    config = yaml.safe_load(f)
with open("config/kafka_config_local.yaml", "r", encoding='utf-8') as f:
    kafka_config = yaml.safe_load(f)

SYMBOLS = config["binance"]["symbols"]
TIMEFRAME = config["binance"]["timeframe"]
CAPITAL = config["binance"]["capital"]
LEVERAGE = config["binance"]["leverage"]
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
logger.info(f"[Kafka] Consumer initialized with bootstrap servers: {KAFKA_BOOTSTRAP}")

def calculate_indicators(df, symbol):
    logger.info(f"Calculating indicators for {symbol} DataFrame with {len(df)} rows")
    print(f"Calculating indicators for {symbol} DataFrame with {len(df)} rows")
    if len(df) < 50:
        logger.info(f"Skipping indicator calculation for {symbol} due to insufficient data")
        print(f"Skipping indicator calculation for {symbol} due to insufficient data")
        return df
    df['RSI'] = talib.RSI(df['close'], timeperiod=14)
    df['MACD'], df['MACD_signal'], df['MACD_hist'] = talib.MACD(df['close'], fastperiod=12, slowperiod=26, signalperiod=9)
    df['ADX'] = talib.ADX(df['high'], df['low'], df['close'], timeperiod=14)
    df["EMA20"] = talib.EMA(df["close"], timeperiod=20)
    df["EMA50"] = talib.EMA(df["close"], timeperiod=50)
    df['ATR'] = talib.ATR(df['high'], df['low'], df['close'], timeperiod=14)
    required_cols = ['RSI', 'MACD', 'MACD_signal', 'MACD_hist', 'ADX', 'EMA20', 'EMA50', 'ATR']
    indicator_cols = required_cols
    df[indicator_cols] = df[indicator_cols].ffill().bfill().interpolate()
    logger.info(f"[Debug] Indicators after update for {symbol}: {df[required_cols].iloc[-1].to_dict()}")
    logger.info(f"After filling NaN, {symbol} DataFrame has {len(df)} rows")
    print(f"After filling NaN, {symbol} DataFrame has {len(df)} rows")

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
    insert_metrics(metrics)
    logger.info(f"[Metrics] Inserted metrics for {symbol}: {metrics}")

    return df

async def main():
    # Dictionaries to manage states per symbol
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
        create_tables()
        logger.info("[Main] Database tables created successfully")
        try:
            client.time()
            logger.info("[Binance Client] API connectivity confirmed")
        except Exception as e:
            logger.error(f"[API Connectivity] Failed: {e}")
            raise Exception("API connectivity test failed")

        # Load historical data for each symbol
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
                    limit = 1000
                    if limit < 500:
                        raise ValueError(f"Unable to fetch sufficient historical data for {symbol}")
            logger.info(f"[Data] Fetched {len(klines)} klines for {symbol}, sample: {klines[0] if klines else 'None'}")
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
            logger.info(f"[Data] Latest timestamp in {symbol} df: {dataframes[symbol].index[-1]}")
            dataframes[symbol] = calculate_indicators(dataframes[symbol], symbol)
            logger.info(f"[Data] {symbol} DataFrame shape after indicators: {dataframes[symbol].shape}, columns: {dataframes[symbol].columns.tolist()}")

            # Train or load LSTM model for each symbol
            logger.info(f"[Main] Training or loading LSTM model for {symbol}")
            models[symbol] = train_or_load_model(dataframes[symbol])
            if models[symbol] is None:
                logger.warning(f"[Model] Using mock model for {symbol} due to failure")
                class MockModel:
                    def predict(self, x, verbose=0):
                        return np.array([[0.5]])
                models[symbol] = MockModel()
            logger.info(f"[Model] Model for {symbol} loaded successfully")

        # Subscribe to topics for all symbols
        topics = [f"{symbol}_candle" for symbol in SYMBOLS]
        consumer.subscribe(topics)
        logger.info(f"[Kafka] Subscribed to topics: {topics}")

        last_log_time = time.time()
        while True:
            logger.info("[Main Loop] Starting iteration")
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
                        send_telegram_alert(f"🔄 Model updated successfully for {symbol}.")
                        lstm_input = prepare_lstm_input(dataframes[symbol])
                        pred = models[symbol].predict(lstm_input, verbose=0)[0][0]
                        logger.info(f"[Debug] Prediction after update: {pred}")
                        send_telegram_alert(f"[Debug] Prediction after update: {pred}")
                continue

            if hasattr(msg, 'error') and msg.error():
                logger.error(f"[Kafka] Consumer error: {msg.error()}")
                print(f"Kafka consumer error: {msg.error()}")
                continue

            if hasattr(msg, 'value') and not msg.error():
                candle_data = msg.value().decode("utf-8")
                topic = msg.topic()
                symbol = next((s for s in SYMBOLS if f"{s}_candle" in topic), None)
                if symbol and symbol in dataframes:
                    candle_df = format_candle(candle_data)
                    if not candle_df.empty:
                        logger.info(f"[Kafka] Received candle for {symbol}: {candle_df.iloc[-1]}")
                        print(f"Received candle for {symbol}: {candle_df.iloc[-1]}")
                        candle_df["timestamp"] = pd.to_datetime(candle_df["timestamp"], unit="ms")
                        candle_df.set_index("timestamp", inplace=True)
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
                            logger.info(f"[Signal] Generated action for {symbol}: {action}, new position: {new_position}")
                            if action == last_action_sent.get(symbol):
                                logger.info(f"[Signal] Ignored repeated action for {symbol}: {action}")
                                continue
                            else:
                                last_action_sent[symbol] = action

                            if action in ["buy", "sell", "close_buy", "close_sell"]:
                                signal_details = {
                                    "symbol": symbol,
                                    "signal_type": action,
                                    "price": float(candle_df["close"].iloc[-1]),
                                    "timestamp": int(candle_df.index[-1].timestamp() * 1000)
                                }
                                insert_signal(signal_details)
                                logger.info(f"[Signal] Stored signal details for {symbol}: {signal_details}")

                                logger.info(f"[Trade] Action for {symbol}: {action}, New Position: {new_position}")
                                print(f"Action for {symbol}: {action}, New Position: {new_position}")
                                print(f"Current position before update for {symbol}: {current_positions[symbol]}")
                                if action in ["buy", "sell"]:
                                    price = candle_df["close"].iloc[-1]
                                    atr = dataframes[symbol]["ATR"].iloc[-1] if 'ATR' in dataframes[symbol].columns else 0
                                    logger.info(f"[Order] Attempting {action} for {symbol}, Price: {price}, ATR: {atr}")
                                    order_details[symbol] = place_order(action, price, atr, client, symbol, CAPITAL, LEVERAGE)
                                    if order_details[symbol]:
                                        insert_trade(order_details[symbol])
                                        last_order_details[symbol] = order_details[symbol]
                                        record_trade_metric(order_details[symbol])
                                        send_telegram_alert(f"Trade executed: {action.upper()} {symbol} at {price}")
                                        logger.info(f"[Order] Successfully placed {action} order for {symbol}")
                                        current_positions[symbol] = new_position
                                        # Initialize trailing stop with trade order ID if available
                                        current_market_price = float(candle_df["close"].iloc[-1])
                                        current_atr = atr
                                        initial_sl_order_id = order_details[symbol].get("orderId") if order_details[symbol] else None
                                        last_sl_order_ids[symbol] = update_trailing_stop(
                                            client=client,
                                            symbol=symbol,
                                            signal=action,
                                            current_price=current_market_price,
                                            atr=current_atr,
                                            base_qty=float(order_details[symbol]["quantity"]),
                                            existing_sl_order_id=None  # Initial call
                                        )
                                        logger.info(f"[Trailing Stop] Initialized for {symbol}, new SL order ID: {last_sl_order_ids[symbol]}")
                                    else:
                                        logger.error(f"[Order] Failed to place {action} order for {symbol}")
                                elif action in ["close_buy", "close_sell"]:
                                    close_side = "sell" if action == "close_buy" else "buy"
                                    price = candle_df["close"].iloc[-1]
                                    atr = dataframes[symbol]["ATR"].iloc[-1] if 'ATR' in dataframes[symbol].columns else 0
                                    logger.info(f"[Order] Attempting {close_side} to close for {symbol}, Price: {price}, ATR: {atr}")
                                    order_details[symbol] = place_order(close_side, price, atr, client, symbol, CAPITAL, LEVERAGE)
                                    if order_details[symbol]:
                                        insert_trade(order_details[symbol])
                                        record_trade_metric(order_details[symbol])
                                        if last_order_details[symbol] and last_order_details[symbol].get("price"):
                                            open_price = float(last_order_details[symbol]["price"])
                                            close_price = float(order_details[symbol]["price"])
                                            side = last_order_details[symbol]["side"]
                                            qty = float(last_order_details[symbol]["quantity"])
                                            pnl = (close_price - open_price) * qty if side == "buy" else (open_price - close_price) * qty
                                        else:
                                            pnl = 0.0
                                        message = f"Trade Closed: {action.upper()} {symbol} at {price}\nPNL: {pnl:.2f} USDT"
                                        send_telegram_alert(message)
                                        logger.info(f"[Order] Closed {action} for {symbol}, PNL: {pnl:.2f} USDT")
                                        last_order_details[symbol] = None
                                        current_positions[symbol] = None
                                    else:
                                        logger.error(f"[Order] Failed to close {action} for {symbol}")
                                print(f"Current position after update for {symbol}: {current_positions[symbol]}")

                            # Dynamic Trailing Stop Loss
                            current_market_price = float(candle_df["close"].iloc[-1])
                            current_atr = dataframes[symbol]["ATR"].iloc[-1] if 'ATR' in dataframes[symbol].columns else 0
                            if current_positions[symbol] is not None and last_sl_order_ids[symbol] is not None:
                                last_sl_order_ids[symbol] = update_trailing_stop(
                                    client=client,
                                    symbol=symbol,
                                    signal=action,
                                    current_price=current_market_price,
                                    atr=current_atr,
                                    base_qty=last_order_details[symbol]["quantity"] if last_order_details[symbol] else 0,
                                    existing_sl_order_id=last_sl_order_ids[symbol]
                                )
                                logger.info(f"[Trailing Stop] Updated for {symbol}, new SL order ID: {last_sl_order_ids[symbol]}")

                        dataframes[symbol] = dataframes[symbol].tail(200)
            await asyncio.sleep(0.1)

    except Exception as e:
        logger.error(f"[Main] Bot error: {e}")
        print(f"Main bot error: {e}")
        send_telegram_alert(f"Bot error: {str(e)}")
    finally:
        logger.info("[Main] Closing Kafka consumer")
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
            logger.error(f"[Main] Unexpected error in main loop: {e}")
            print(f"Unexpected error: {e}")