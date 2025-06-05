import asyncio
import yaml
import pandas as pd
import talib
import logging
from confluent_kafka import Consumer
from binance.client import Client
from src.data_ingestion.data_formatter import format_candle
from src.processing_core.lstm_model import train_or_load_model
from src.processing_core.signal_generator import check_signal
from src.trade_execution.order_manager import place_order
from src.database.db_handler import insert_trade
from src.monitoring.metrics import record_trade_metric
from src.monitoring.alerting import send_telegram_alert
import platform
import os

# Set up logging
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    filename="logs/trading_bot.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
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

# Initialize Binance client
client = Client(config["binance"]["api_key"], config["binance"]["api_secret"])

# Initialize Kafka consumer
consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "group.id": "trading_bot_group",
    "auto.offset.reset": "latest",
    "security.protocol": "PLAINTEXT",
    "api.version.request": True,
    "broker.version.fallback": "0.10.0"
})

# Function to calculate technical indicators
def calculate_indicators(df):
    logger.info(f"Calculating indicators for DataFrame with {len(df)} rows")
    print(f"Calculating indicators for DataFrame with {len(df)} rows")
    if len(df) < 26:
        logger.info("Skipping indicator calculation due to insufficient data")
        print("Skipping indicator calculation due to insufficient data")
        return df
    df['RSI'] = talib.RSI(df['close'], timeperiod=14)
    macd, signal, hist = talib.MACD(df['close'], fastperiod=12, slowperiod=26, signalperiod=9)
    df['MACD'] = macd
    df['MACD_signal'] = signal
    df['MACD_hist'] = hist
    df['ADX'] = talib.ADX(df['high'], df['low'], df['close'], timeperiod=14)
    df['ATR'] = talib.ATR(df['high'], df['low'], df['close'], timeperiod=14)
    df['EMA20'] = talib.EMA(df['close'], timeperiod=20)
    df['EMA50'] = talib.EMA(df['close'], timeperiod=50)
    df = df.dropna()
    logger.info(f"After dropna, DataFrame has {len(df)} rows")
    print(f"After dropna, DataFrame has {len(df)} rows")
    return df

async def main():
    global client
    df = pd.DataFrame()
    order_details = None
    current_position = None  # Track current position state: None, "buy", or "sell"
    try:
        # Load historical data
        klines = client.futures_klines(symbol=SYMBOL, interval=TIMEFRAME, limit=500)
        data_hist = pd.DataFrame(klines, columns=["open_time", "open", "high", "low", "close", "volume",
                                                 "close_time", "quote_asset_vol", "num_trades", "taker_buy_base_vol",
                                                 "taker_buy_quote_vol", "ignore"])
        data_hist = data_hist[["open_time", "open", "high", "low", "close", "volume"]].astype(float)
        df = data_hist.rename(columns={"open_time": "timestamp"})
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
        
        # Compute indicators for historical data
        df = calculate_indicators(df)
        
        # Train or load LSTM model
        model = train_or_load_model(df)
        if model is None:
            logger.error("Failed to initialize model. Exiting...")
            print("Failed to initialize model. Exiting...")
            return

        consumer.subscribe([KAFKA_TOPIC])
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Kafka consumer error: {msg.error()}")
                print(f"Kafka consumer error: {msg.error()}")
                continue
            candle_df = format_candle(msg.value().decode("utf-8"))
            if not candle_df.empty:
                logger.info(f"Received candle: {candle_df.iloc[-1]}")
                print(f"Received candle: {candle_df.iloc[-1]}")
                df = pd.concat([df, candle_df], ignore_index=True).tail(250)
                df = calculate_indicators(df)
                if len(df) >= 26:
                    action, new_position = check_signal(df, model, current_position)
                    logger.info(f"Action: {action}, New Position: {new_position}")
                    print(f"Action: {action}, New Position: {new_position}")
                    if action in ["buy", "sell"]:
                        price = candle_df["close"].iloc[-1]
                        atr = df["ATR"].iloc[-1] if 'ATR' in df.columns else 0
                        order_details = place_order(action, price, atr, client, SYMBOL, CAPITAL, LEVERAGE)
                        if order_details:
                            insert_trade(order_details)
                            record_trade_metric(order_details)
                            send_telegram_alert(f"Trade executed: {action.upper()} {SYMBOL} at {price}")
                            current_position = new_position  # Update position state
                    elif action in ["close_buy", "close_sell"]:
                        close_side = "sell" if action == "close_buy" else "buy"
                        price = candle_df["close"].iloc[-1]
                        atr = df["ATR"].iloc[-1] if 'ATR' in df.columns else 0
                        order_details = place_order(close_side, price, atr, client, SYMBOL, CAPITAL, LEVERAGE)
                        if order_details:
                            insert_trade(order_details)
                            record_trade_metric(order_details)
                            send_telegram_alert(f"Position closed: {action.upper()} {SYMBOL} at {price}")
                            current_position = None  # Reset position state
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
        asyncio.run(main())