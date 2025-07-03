import eventlet
eventlet.monkey_patch()
import asyncio
import yaml
import pandas as pd
import talib
import logging
from logging.handlers import RotatingFileHandler
from binance.um_futures import UMFutures
from binance.websocket.um_futures.websocket_client import UMFuturesWebsocketClient
from confluent_kafka import Consumer
from src.data_ingestion.data_formatter import format_candle
from src.processing_core.lstm_model import train_or_load_model
from src.processing_core.signal_generator import check_signal, prepare_lstm_input
from src.trade_execution.order_manager import place_order, update_trailing_stop, init_trailing_stop_manager, EnhancedOrderManager
from src.trade_execution.sync_orders import get_current_atr, sync_binance_trades_with_postgres
from src.database.db_handler import insert_trade, insert_signal, insert_metrics, create_tables, insert_price_data, clean_old_data
from src.monitoring.metrics import record_trade_metric
from src.monitoring.alerting import send_telegram_alert
from src.performance.tracker import performance_tracker_loop
import sys
import os
import time
import numpy as np
import threading
import io
import platform
import json
from datetime import datetime
from rich.console import Console
from rich.table import Table
from rich.logging import RichHandler
import psycopg2.pool

# Initialize rich console for enhanced logging
console = Console()

# Force UTF-8 encoding
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8-sig")
sys.stderr.reconfigure(encoding="utf-8-sig")

# Set up logging
os.makedirs("logs", exist_ok=True)
log_level = logging.DEBUG if os.getenv("DEBUG_MODE") == "true" else logging.INFO
logging.basicConfig(
    level=log_level,
    format='%(asctime)s - %(levelname)s - [%(name)s] %(message)s',
    handlers=[
        RotatingFileHandler('logs/trading_bot.log', maxBytes=1_000_000, backupCount=3, encoding="utf-8-sig"),
        RichHandler(console=console, rich_tracebacks=True)
    ]
)
logger = logging.getLogger(__name__)

# Load configuration
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
config_path = os.path.join(project_root, 'config', 'config.yaml')
kafka_config_path = os.path.join(project_root, 'config', 'kafka_config_local.yaml')

for config_file in [config_path, kafka_config_path]:
    if not os.path.exists(config_file):
        logger.error(f"❌ [Config Error] Configuration file not found: {config_file}")
        raise FileNotFoundError(f"Configuration file not found: {config_file}")

try:
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    with open(kafka_config_path, 'r', encoding='utf-8') as f:
        kafka_config = yaml.safe_load(f)
except Exception as e:
    logger.error(f"❌ [Config Error] Failed to load configuration files: {e}")
    raise

required_top_level_keys = ["binance"]
for key in required_top_level_keys:
    if key not in config or not config[key]:
        logger.error(f"❌ [Config Error] Missing or invalid key in config.yaml: {key}")
        raise ValueError(f"Missing or invalid key in config.yaml: {key}")

binance_config = config["binance"]
required_binance_keys = ["symbols", "timeframe", "capital", "leverage"]
for key in required_binance_keys:
    if key not in binance_config or not binance_config[key]:
        logger.error(f"❌ [Config Error] Missing or invalid '{key}' under 'binance' in config.yaml")
        raise ValueError(f"Missing or invalid '{key}' under 'binance' in config.yaml")

SYMBOLS = binance_config["symbols"]
TIMEFRAME = binance_config["timeframe"]
CAPITAL = binance_config["capital"]
LEVERAGE = binance_config["leverage"]
KAFKA_BOOTSTRAP = kafka_config["kafka"]["bootstrap_servers"]
MODEL_UPDATE_INTERVAL = 900  # 15 minutes
METRICS_UPDATE_INTERVAL = 300  # 5 minutes

# Initialize Binance client
try:
    client = UMFutures(
        key=binance_config["api_key"],
        secret=binance_config["api_secret"],
        base_url=binance_config["base_url"]
    )
except Exception as e:
    logger.error(f"❌ [Binance Client] Failed to initialize client: {e}")
    raise

# Initialize trailing stop manager and order manager
ts_manager = init_trailing_stop_manager(client)
order_manager = EnhancedOrderManager(client, SYMBOLS)

# Synchronisation initiale des trades
logger.info("[MainBot] Syncing Binance trades with PostgreSQL and internal tracker... 🚀")
try:
    sync_binance_trades_with_postgres(client, SYMBOLS, ts_manager)
except psycopg2.pool.PoolError as e:
    logger.error(f"❌ [MainBot] Failed to sync trades due to connection pool exhaustion: {e}")
    send_telegram_alert(f"Failed to sync trades: connection pool exhausted")
    raise
logger.info("[MainBot] Initial trade sync completed. ✅")

# Initialize Kafka consumer
try:
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "trading_bot_group",
        "auto.offset.reset": "latest",
        "security.protocol": "PLAINTEXT"
    })
    logger.info(f"[Kafka] Consumer initialized with bootstrap servers: {KAFKA_BOOTSTRAP} 🛠️")
except Exception as e:
    logger.error(f"❌ [Kafka] Failed to initialize consumer: {e}")
    raise

def handle_order_update(message):
    try:
        if 'e' in message and message['e'] == 'ORDER_TRADE_UPDATE':
            order = message['o']
            order_data = {
                'order_id': str(order['i']),
                'symbol': order['s'],
                'side': order['S'].lower(),
                'quantity': float(order['q']),
                'price': float(order['ap'] or order['p']),
                'timestamp': order['T'],
                'status': order['X'].lower(),
                'is_trailing': order['o'] == 'TRAILING_STOP_MARKET',
                'trade_id': order.get('c', str(int(order['T'])))
            }
            if order_data['status'] == 'filled':
                trade_data = {
                    'order_id': order_data['order_id'],
                    'symbol': order_data['symbol'],
                    'side': order_data['side'],
                    'quantity': order_data['quantity'],
                    'price': order_data['price'],
                    'stop_loss': None,
                    'take_profit': None,
                    'timestamp': order_data['timestamp'],
                    'pnl': 0.0,
                    'is_trailing': order_data['is_trailing'],
                    'trade_id': order_data['trade_id']
                }
                insert_trade(trade_data)
                
                # Create a table for the trade
                table = Table(title=f"Trade Executed for {order_data['symbol']}")
                table.add_column("Field", style="cyan")
                table.add_column("Value", style="magenta")
                table.add_row("Order ID", order_data['order_id'])
                table.add_row("Symbol", order_data['symbol'])
                table.add_row("Side", order_data['side'])
                table.add_row("Quantity", f"{order_data['quantity']:.2f}")
                table.add_row("Price", f"{order_data['price']:.4f}")
                table.add_row("Timestamp", str(order_data['timestamp']))
                console.log(table)
                
                if not order_data['is_trailing']:
                    atr = get_current_atr(client, order_data['symbol'])
                    if atr <= 0:
                        logger.error(f"❌ [Trailing Stop] Invalid ATR for {order_data['symbol']}: {atr}")
                        return
                    position_type = 'long' if order_data['side'] == 'buy' else 'short'
                    ts_manager.initialize_trailing_stop(
                        symbol=order_data['symbol'],
                        entry_price=order_data['price'],
                        position_type=position_type,
                        quantity=order_data['quantity'],
                        atr=atr,
                        trade_id=order_data['trade_id']
                    )
                    logger.info(f"Initialized trailing stop for WebSocket trade {order_data['order_id']} ({order_data['symbol']}) 📈")
                if order_data['is_trailing']:
                    current_price = order_manager.get_current_price(order_data['symbol'])
                    if current_price:
                        ts_manager.update_trailing_stop(order_data['symbol'], current_price, trade_id=order_data['trade_id'])
                        logger.info(f"Updated trailing stop for {order_data['symbol']} at price {current_price:.4f} 🔄")
    except Exception as e:
        logger.error(f"❌ Error handling WebSocket order update: {str(e)}")

def start_websocket():
    max_retries = 5
    retry_delay = 5  # Initial delay in seconds
    while True:
        try:
            ws_client = UMFuturesWebsocketClient()
            listen_key = client.new_listen_key()['listenKey']
            ws_client.user_data(
                listen_key=listen_key,
                id=1,
                callback=handle_order_update
            )
            logger.info("[WebSocket] WebSocket client initialized. 🌐")
            while True:
                eventlet.sleep(3600)
                try:
                    client.renew_listen_key(listen_key)
                    logger.info("[WebSocket] Listen key renewed. 🔄")
                except Exception as e:
                    logger.error(f"❌ [WebSocket] Failed to renew listen key: {e}")
                    send_telegram_alert(f"Failed to renew WebSocket listen key: {str(e)}")
                    break  # Retry initializing WebSocket
        except Exception as e:
            logger.error(f"❌ [WebSocket] Failed to initialize WebSocket: {e}")
            send_telegram_alert(f"WebSocket initialization failed: {str(e)}")
            if max_retries > 0:
                logger.info(f"[WebSocket] Retrying in {retry_delay} seconds... (Attempts left: {max_retries})")
                time.sleep(retry_delay)
                max_retries -= 1
                retry_delay *= 2  # Exponential backoff
            else:
                logger.error("[WebSocket] Max retries reached. WebSocket will not be initialized.")
                send_telegram_alert("WebSocket failed after maximum retries. Please check connectivity.")
                break

websocket_thread = threading.Thread(target=start_websocket, daemon=True)
websocket_thread.start()

def calculate_indicators(df, symbol):
    logger.info(f"Calculating indicators for {symbol} DataFrame with {len(df)} rows 📊")
    if len(df) < 50:
        logger.info(f"⚠️ Skipping indicator calculation for {symbol} due to insufficient data")
        return df

    df = df.dropna(subset=['open', 'high', 'low', 'close', 'volume']).copy()
    if len(df) < 50:
        logger.warning(f"⚠️ Data cleaned, insufficient rows for {symbol}: {len(df)}")
        return df

    try:
        df['RSI'] = talib.RSI(df['close'], timeperiod=14)
        df['MACD'], df['MACD_signal'], df['MACD_hist'] = talib.MACD(df['close'], fastperiod=12, slowperiod=26, signalperiod=9)
        df['ADX'] = talib.ADX(df['high'], df['low'], df['close'], timeperiod=14)
        df['EMA20'] = talib.EMA(df['close'], timeperiod=20)
        df['EMA50'] = talib.EMA(df['close'], timeperiod=50)
        df['ATR'] = talib.ATR(df['high'], df['low'], df['close'], timeperiod=14)
        df['ROC'] = talib.ROC(df['close'], timeperiod=5) * 100

        # Validate ADX and ROC
        if df['ADX'].iloc[-1] > 90:
            logger.warning(f"[Indicators] Unusually high ADX for {symbol}: {df['ADX'].iloc[-1]:.2f}. Check input data: high={df['high'].iloc[-1]:.2f}, low={df['low'].iloc[-1]:.2f}, close={df['close'].iloc[-1]:.2f}")
        if abs(df['ROC'].iloc[-1]) > 30:
            logger.warning(f"[Indicators] Unusually high ROC for {symbol}: {df['ROC'].iloc[-1]:.2f}%. Check close prices: {df['close'].iloc[-6:].tolist()}")

        # Remplir les NaN avec la méthode forward fill, puis backward fill
        required_cols = ['RSI', 'MACD', 'MACD_signal', 'MACD_hist', 'ADX', 'EMA20', 'EMA50', 'ATR', 'ROC']
        df[required_cols] = df[required_cols].ffill().bfill()

        # Vérifier si des NaN persist
        if df[required_cols].isna().any().any():
            logger.warning(f"⚠️ [Indicators] NaN values persist in indicators for {symbol} after filling")

        metrics = {
            "symbol": symbol,
            "timestamp": int(df.index[-1].timestamp() * 1000),
            "rsi": float(df['RSI'].iloc[-1]),
            "macd": float(df['MACD'].iloc[-1]),
            "adx": float(df['ADX'].iloc[-1]),
            "ema20": float(df['EMA20'].iloc[-1]),
            "ema50": float(df['EMA50'].iloc[-1]),
            "atr": float(df['ATR'].iloc[-1]),
            "roc": float(df['ROC'].iloc[-1])
        }
        insert_metrics(symbol, metrics)

        # Create a table for indicators
        table = Table(title=f"Technical Indicators for {symbol}")
        table.add_column("Indicator", style="cyan")
        table.add_column("Value", style="magenta")
        table.add_row("RSI", f"{metrics['rsi']:.2f}")
        table.add_row("MACD", f"{metrics['macd']:.4f}")
        table.add_row("ADX", f"{metrics['adx']:.2f}")
        table.add_row("EMA20", f"{metrics['ema20']:.4f}")
        table.add_row("EMA50", f"{metrics['ema50']:.4f}")
        table.add_row("ATR", f"{metrics['atr']:.4f}")
        table.add_row("ROC", f"{metrics['roc']:.2f}%")
        console.log(table)

    except Exception as e:
        logger.error(f"❌ [Indicators] Error calculating indicators for {symbol}: {e}")
        import traceback
        logger.error(traceback.format_exc())

    return df
async def main():
    dataframes = {symbol: pd.DataFrame() for symbol in SYMBOLS}
    order_details = {symbol: None for symbol in SYMBOLS}
    current_positions = {symbol: None for symbol in SYMBOLS}
    last_order_details = {symbol: None for symbol in SYMBOLS}
    last_model_updates = {symbol: time.time() for symbol in SYMBOLS}
    last_action_sent = {symbol: (None, 0) for symbol in SYMBOLS}  # Store (action, timestamp)
    last_sl_order_ids = {symbol: None for symbol in SYMBOLS}
    models = {symbol: None for symbol in SYMBOLS}
    scalers = {symbol: None for symbol in SYMBOLS}
    last_sync_time = time.time()
    sync_interval = 300  # 5 minutes
    last_metrics_update = {symbol: 0 for symbol in SYMBOLS}

    try:
        logger.info("[Main] Creating database tables 🛠️")
        schema_path = os.path.join(project_root, 'src', 'database', 'schema.sql')
        if not os.path.exists(schema_path):
            logger.error(f"❌ [Config Error] Database schema file not found: {schema_path}")
            raise FileNotFoundError(f"Database schema file not found: {schema_path}")
        create_tables()
        logger.info("[Main] Database tables created successfully ✅")
    except psycopg2.pool.PoolError as e:
        logger.error(f"❌ [Main] Error creating database tables due to connection pool exhaustion: {e}")
        send_telegram_alert(f"Error creating database tables: connection pool exhausted")
        raise
    except Exception as e:
        logger.error(f"❌ [Main] Error creating database tables: {e}")
        raise

    try:
        client.time()
        logger.info("[Binance Client] API connectivity confirmed 🌐")
    except Exception as e:
        logger.error(f"❌ [API Connectivity] Failed: {e}")
        raise Exception("API connectivity test failed")

    for symbol in SYMBOLS:
        logger.info(f"[Main] Fetching historical data for {symbol} with timeframe {TIMEFRAME} 📈")
        klines = []
        limit = 1500
        start_time = None
        while len(klines) < 500:
            try:
                new_klines = client.klines(symbol=symbol, interval=TIMEFRAME, limit=limit, startTime=start_time)
                if not new_klines or len(new_klines) == 0:
                    break
                klines.extend(new_klines)
                start_time = int(new_klines[-1][0]) + 1
                logger.info(f"[Data Fetch] Fetched {len(new_klines)} klines for {symbol}, total: {len(klines)} 📊")
            except Exception as e:
                logger.error(f"❌ [Data Fetch] Failed for {symbol} with limit={limit}: {e}")
                limit = max(500, limit - 100)
                if limit < 500:
                    raise ValueError(f"Unable to fetch sufficient historical data for {symbol}")
        if len(klines) < 101:
            logger.error(f"❌ [Data] Insufficient historical data fetched for {symbol}: {len(klines)} rows")
            raise ValueError(f"Insufficient historical data for {symbol}")
        data_hist = pd.DataFrame(klines, columns=["open_time", "open", "high", "low", "close", "volume",
                                                "close_time", "quote_asset_vol", "num_trades", "taker_buy_base_vol",
                                                "taker_buy_quote_vol", "ignore"])
        data_hist = data_hist[["open_time", "open", "high", "low", "close", "volume"]].astype(float)
        dataframes[symbol] = data_hist.rename(columns={"open_time": "timestamp"})
        dataframes[symbol]["timestamp"] = pd.to_datetime(dataframes[symbol]["timestamp"], unit="ms")
        dataframes[symbol].set_index("timestamp", inplace=True)
        dataframes[symbol] = dataframes[symbol].tail(500)
        dataframes[symbol] = calculate_indicators(dataframes[symbol], symbol)

        logger.info(f"[Main] Training or loading LSTM model for {symbol} 🤖")
        models[symbol], scalers[symbol] = train_or_load_model(dataframes[symbol], symbol)
        if models[symbol] is None or scalers[symbol] is None:
            logger.warning(f"⚠️ [Model] Using mock model for {symbol} due to failure")
            class MockModel:
                def predict(self, x, verbose=0):
                    return np.array([[0.5]])
            models[symbol] = MockModel()
            scalers[symbol] = None

    topics = [f"{symbol}_candle" for symbol in SYMBOLS]
    try:
        max_retries = 5
        retry_delay = 5
        for attempt in range(max_retries):
            try:
                consumer.subscribe(topics)
                logger.info(f"[Kafka] Subscribed to topics: {topics} 📡")
                break
            except Exception as e:
                logger.error(f"❌ [Kafka] Failed to subscribe to topics (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    retry_delay *= 2
                else:
                    logger.error("[Kafka] Max retries reached for topic subscription")
                    raise
    except Exception as e:
        logger.error(f"❌ [Kafka] Failed to subscribe to topics: {e}")
        raise

    logger.info("[Main] Waiting for Kafka topics to be created...")
    time.sleep(10)  # Wait 10 seconds for kafka_consumer.py to create topics

    last_log_time = time.time()
    iteration_count = 0
    try:
        while True:
            current_time = time.time()
            if current_time - last_sync_time >= sync_interval:
                logger.info("[MainBot] Running periodic trade sync... 🚀")
                try:
                    sync_binance_trades_with_postgres(client, SYMBOLS, ts_manager)
                except psycopg2.pool.PoolError as e:
                    logger.error(f"❌ [MainBot] Failed to sync trades due to connection pool exhaustion: {e}")
                    send_telegram_alert(f"Failed to sync trades: connection pool exhausted")
                last_sync_time = current_time

            if iteration_count % 10 == 0:
                logger.info("[Main Loop] Starting iteration 🔄")
            iteration_count += 1
            msg = consumer.poll(1.0)

            if msg is None:
                if current_time - last_log_time >= 5:
                    logger.info("[Main Loop] Running, awaiting data ⏳")
                    last_log_time = current_time
                for symbol in SYMBOLS:
                    if current_time - last_model_updates[symbol] >= MODEL_UPDATE_INTERVAL and len(dataframes[symbol]) >= 101:
                        logger.info(f"[Main] Updating LSTM model for {symbol} with new data 🤖")
                        models[symbol], scalers[symbol] = train_or_load_model(dataframes[symbol], symbol)
                        if models[symbol] is None or scalers[symbol] is None:
                            logger.error(f"❌ [Model] No valid model available for {symbol}, using mock prediction")
                            class MockModel:
                                def predict(self, x, verbose=0):
                                    return np.array([[0.5]])
                            models[symbol] = MockModel()
                            scalers[symbol] = None
                        last_model_updates[symbol] = current_time
                        send_telegram_alert(f"Bingo ! Model updated successfully for {symbol}.")
                        if scalers[symbol]:
                            lstm_input = prepare_lstm_input(dataframes[symbol])
                            pred = models[symbol].predict(lstm_input, verbose=0)[0][0]
                            logger.info(f"[Look] Prediction after update: {pred:.4f} 📈")
                            send_telegram_alert(f"[Look] Prediction after update: {pred:.4f}")

                await asyncio.sleep(0.1)
                continue

            if msg.error():
                logger.error(f"❌ [Kafka] Consumer error: {msg.error()}")
                continue

            try:
                candle_data = json.loads(msg.value().decode("utf-8")) if msg.value() else None
                if not candle_data or not isinstance(candle_data, dict):
                    logger.error(f"[Kafka] Invalid candle data for topic {msg.topic()}: {candle_data}")
                    continue
                topic = msg.topic()
                symbol = next((s for s in SYMBOLS if f"{s}_candle" in topic), None)
                if not symbol or symbol not in dataframes:
                    logger.error(f"❌ [Kafka] Invalid symbol for topic {topic}")
                    continue

                # Validation des clés Kafka
                required_keys = ['T', 'o', 'h', 'l', 'c', 'v']
                alt_required_keys = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
                if not all(key in candle_data for key in required_keys) and not all(key in candle_data for key in alt_required_keys):
                    logger.error(f"❌ [Kafka] Missing required keys in candle data for {symbol}: {candle_data}")
                    continue

                candle_df = format_candle(candle_data, symbol)
                if candle_df.empty or "close" not in candle_df.columns:
                    logger.error(f"❌ [Kafka] Invalid or empty candle data for {symbol}: {candle_data}")
                    continue
                if not isinstance(candle_df.index, pd.DatetimeIndex):
                    logger.error(f"❌ [Kafka] candle_df index is not a DatetimeIndex for {symbol}: {type(candle_df.index)}")
                    continue
                insert_price_data(candle_df, symbol)
                dataframes[symbol] = pd.concat([dataframes[symbol], candle_df], ignore_index=False)

                # Calculer les indicateurs immédiatement après une nouvelle bougie si les données sont suffisantes
                if len(dataframes[symbol]) >= 50:
                    dataframes[symbol] = calculate_indicators(dataframes[symbol], symbol)
                    last_metrics_update[symbol] = current_time

                if len(dataframes[symbol]) >= 101:
                    logger.debug(f"[main_bot] Calling check_signal for {symbol} with dataframe length {len(dataframes[symbol])}")
                    action, new_position, confidence, confidence_factors = check_signal(
                        df=dataframes[symbol],
                        model=models[symbol],
                        current_position=current_positions[symbol],
                        last_order_details=last_order_details[symbol],
                        symbol=symbol,
                        last_action_sent=last_action_sent[symbol][0],
                        config=config
                    )
                    if action == last_action_sent[symbol][0]:
                        logger.info(f"[Signal] Ignored repeated action for {symbol}: {action} 🔄")
                        continue
                    timestamp = int(candle_df.index[-1].timestamp() * 1000)
                    last_action_sent[symbol] = (action, timestamp)  # Update with action and timestamp

                    if action in ["buy", "sell", "close_buy", "close_sell"]:
                        # Create a table for the signal
                        signal_table = Table(title=f"Signal Generated for {symbol}")
                        signal_table.add_column("Field", style="cyan")
                        signal_table.add_column("Value", style="magenta")
                        signal_table.add_row("Action", action)
                        signal_table.add_row("Symbol", symbol)
                        signal_table.add_row("Timestamp", str(timestamp))
                        signal_table.add_row("Confidence Score", f"{confidence:.2f}")
                        signal_table.add_row("Confidence Factors", ", ".join(confidence_factors) if confidence_factors else "None")
                        console.log(signal_table)

                        # Insérer le signal
                        try:
                            insert_signal(symbol, action, float(candle_df["close"].iloc[-1]), (CAPITAL * LEVERAGE) / float(candle_df["close"].iloc[-1]), dataframes[symbol]['strategy_mode'].iloc[-1] if 'strategy_mode' in dataframes[symbol].columns else "unknown", timestamp, confidence)
                            logger.info(f"[Signal] Generated signal {action} for {symbol} with confidence {confidence:.2f}")
                        except psycopg2.pool.PoolError as e:
                            logger.error(f"❌ [Signal] Failed to insert signal for {symbol} due to connection pool exhaustion: {e}")
                            send_telegram_alert(f"Failed to insert signal for {symbol}: connection pool exhausted")
                            continue

                        if action in ["buy", "sell"]:
                            try:
                                price = float(candle_df["close"].iloc[-1])
                                atr = float(dataframes[symbol]["ATR"].iloc[-1]) if 'ATR' in dataframes[symbol].columns and not np.isnan(dataframes[symbol]["ATR"].iloc[-1]) else 0
                                if atr <= 0:
                                    logger.error(f"❌ [Order] Invalid ATR for {symbol}: {atr}")
                                    continue
                                order_details[symbol] = order_manager.place_enhanced_order(action, symbol, CAPITAL, LEVERAGE, trade_id=str(timestamp))
                                if order_details[symbol]:
                                    insert_trade(order_details[symbol])
                                    last_order_details[symbol] = order_details[symbol]
                                    record_trade_metric(order_details[symbol])
                                    send_telegram_alert(f"Trade executed: {action.upper()} {symbol} at {price} 💰")
                                    current_positions[symbol] = new_position
                                    current_market_price = float(candle_df["close"].iloc[-1])
                                    last_sl_order_ids[symbol] = update_trailing_stop(
                                        client=client,
                                        symbol=symbol,
                                        signal=action,
                                        current_price=current_market_price,
                                        atr=atr,
                                        base_qty=float(order_details[symbol]["quantity"]),
                                        existing_sl_order_id=last_sl_order_ids[symbol],
                                        trade_id=str(timestamp)
                                    )

                                    # Create a table for the order
                                    table = Table(title=f"Order Placed for {symbol}")
                                    table.add_column("Field", style="cyan")
                                    table.add_column("Value", style="magenta")
                                    table.add_row("Order ID", order_details[symbol].get("order_id", "N/A"))
                                    table.add_row("Side", order_details[symbol].get("side", "N/A"))
                                    table.add_row("Quantity", f"{order_details[symbol].get('quantity', 0):.2f}")
                                    table.add_row("Price", f"{order_details[symbol].get('price', 0):.4f}")
                                    table.add_row("Confidence Score", f"{confidence:.2f}")
                                    table.add_row("Confidence Factors", ", ".join(confidence_factors) if confidence_factors else "None")
                                    console.log(table)
                                else:
                                    logger.error(f"❌ [Order] Failed to place {action} order for {symbol}")
                            except (TypeError, ValueError, IndexError, psycopg2.pool.PoolError) as e:
                                logger.error(f"❌ [Order] Error placing {action} order for {symbol}: {e}")
                                send_telegram_alert(f"Error placing {action} order for {symbol}: {str(e)}")
                        elif action in ["close_buy", "close_sell"]:
                            try:
                                close_side = "sell" if action == "close_buy" else "buy"
                                price = float(candle_df["close"].iloc[-1])
                                atr = float(dataframes[symbol]["ATR"].iloc[-1]) if 'ATR' in dataframes[symbol].columns and not np.isnan(dataframes[symbol]["ATR"].iloc[-1]) else 0
                                if atr <= 0:
                                    logger.error(f"❌ [Order] Invalid ATR for {symbol}: {atr}")
                                    continue
                                order_details[symbol] = order_manager.place_enhanced_order(close_side, symbol, CAPITAL, LEVERAGE, trade_id=str(timestamp))
                                if order_details[symbol]:
                                    if last_order_details[symbol] and last_order_details[symbol].get("price"):
                                        open_price = float(last_order_details[symbol]["price"])
                                        close_price = float(order_details[symbol]["price"])
                                        side = last_order_details[symbol]["side"]
                                        qty = float(last_order_details[symbol]["quantity"])
                                        pnl = (close_price - open_price) * qty if side == "buy" else (open_price - close_price) * qty
                                        order_details[symbol]["pnl"] = pnl
                                    # Create a table for the order
                                    table = Table(title=f"Trade Closed for {symbol}")
                                    table.add_column("Field", style="cyan")
                                    table.add_column("Value", style="magenta")
                                    table.add_row("Order ID", order_details[symbol].get("order_id", "N/A"))
                                    table.add_row("Side", close_side)
                                    table.add_row("Quantity", f"{order_details[symbol].get('quantity', 0):.2f}")
                                    table.add_row("Price", f"{order_details[symbol].get('price', 0):.4f}")
                                    table.add_row("PNL", f"{order_details[symbol].get('pnl', 0):.2f}")
                                    table.add_row("Confidence Score", f"{confidence:.2f}")
                                    table.add_row("Confidence Factors", ", ".join(confidence_factors) if confidence_factors else "None")
                                    console.log(table)
                                current_positions[symbol] = None
                            except (TypeError, ValueError, IndexError, psycopg2.pool.PoolError) as e:
                                logger.error(f"❌ [Order] Error closing {action} order for {symbol}: {e}")
                                send_telegram_alert(f"Error closing {action} order for {symbol}: {str(e)}")
            except Exception as e:
                logger.error(f"❌ [Kafka] Exception while processing candle for {symbol}: {e}")
                continue
            await asyncio.sleep(0.1)

    except Exception as e:
        logger.error(f"❌ [Main] Bot error: {e}")
        send_telegram_alert(f"Bot error: {str(e)}")
    finally:
        logger.info("[Main] Closing Kafka consumer 🛑")
        consumer.close()

def tracker_loop():
    last_cleanup_date = None
    while True:
        try:
            now = datetime.now()
            # Nettoyer une fois par jour à minuit
            if now.hour == 0 and (last_cleanup_date is None or last_cleanup_date != now.date()):
                clean_old_data()
                last_cleanup_date = now.date()
            # Autres tâches de suivi/performance ici...
            time.sleep(60)  # Attendre 1 minute avant la prochaine vérification
        except Exception as e:
            logger.error(f"[Tracker] Error in tracker loop: {e}", exc_info=True)

if platform.system() == "Emscripten":
    asyncio.ensure_future(main())
else:
    if __name__ == "__main__":
        performance_thread = threading.Thread(target=performance_tracker_loop, args=(client, SYMBOLS), daemon=True)
        performance_thread.start()
        asyncio.run(main())