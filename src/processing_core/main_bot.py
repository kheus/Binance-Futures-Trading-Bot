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
from src.trade_execution.order_manager import place_order, update_trailing_stop, init_trailing_stop_manager
from src.trade_execution.sync_orders import get_current_atr, get_current_price
from src.database.db_handler import insert_trade, insert_signal, insert_metrics, create_tables, insert_price_data, get_db_connection
from src.monitoring.metrics import record_trade_metric
from monitoring.alerting import send_telegram_alert
from src.trade_execution.sync_orders import sync_binance_trades_with_postgres
from src.performance.tracker import performance_tracker_loop
import sys
import os
import time
import numpy as np
import threading
import io
import platform
import json

# Force UTF-8 encoding
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8-sig")
sys.stderr.reconfigure(encoding="utf-8-sig")

# Set up logging
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(name)s] %(message)s',
    handlers=[
        RotatingFileHandler('logs/trading_bot.log', maxBytes=1_000_000, backupCount=3, encoding="utf-8-sig"),
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
    raise

# Validate configuration
required_top_level_keys = ["binance"]
for key in required_top_level_keys:
    if key not in config or not config[key]:
        logger.error(f"[Config Error] Missing or invalid key in config.yaml: {key}")
        raise ValueError(f"Missing or invalid key in config.yaml: {key}")

binance_config = config["binance"]
required_binance_keys = ["symbols", "timeframe", "capital", "leverage"]
for key in required_binance_keys:
    if key not in binance_config or not binance_config[key]:
        logger.error(f"[Config Error] Missing or invalid '{key}' under 'binance' in config.yaml")
        raise ValueError(f"Missing or invalid '{key}' under 'binance' in config.yaml")

SYMBOLS = binance_config["symbols"]
TIMEFRAME = binance_config["timeframe"]
CAPITAL = binance_config["capital"]
LEVERAGE = binance_config["leverage"]
KAFKA_BOOTSTRAP = kafka_config["kafka"]["bootstrap_servers"]
MODEL_UPDATE_INTERVAL = 900  # 15 minutes

# Initialize Binance client
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
logger.info(f"[Debug] sys.path: {sys.path}")
try:
    from src.trade_execution.binance_client import init_binance_client
    logger.info("[Debug] binance_client module imported successfully")
except ImportError as e:
    logger.error(f"[Import Error] Failed to import binance_client: {e}")
    raise

client = UMFutures(
    key="f52c3046240c9514626cf7619ea6bb93f329e2ad39dac256291f48655e750545",
    secret="20fe2ccf55a7114e576e5830e6ebadbdfdb66df849a326d50ebfb2ca394ce7ec",
    base_url="https://testnet.binancefuture.com"
)

# Initialize trailing stop manager
ts_manager = init_trailing_stop_manager(client)

# Synchronisation initiale des trades
logger.info("[MainBot] Syncing Binance trades with PostgreSQL and internal tracker...")
sync_binance_trades_with_postgres(client, SYMBOLS, ts_manager)
logger.info("[MainBot] Initial trade sync completed.")

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
    raise

# Initialize WebSocket client
def handle_order_update(message):
    """Gère les mises à jour des ordres via WebSocket."""
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
                'is_trailing': order['o'] == 'TRAILING_STOP_MARKET'
            }
            if order_data['status'] == 'filled':
                query = "SELECT order_id, symbol FROM trades WHERE order_id = %s AND symbol = %s"
                with get_db_connection() as conn:
                    existing = pd.read_sql_query(query, conn, params=(order_data['order_id'], order_data['symbol']))
                if existing.empty:
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
                        'is_trailing': order_data['is_trailing']
                    }
                    insert_trade(trade_data)
                    logger.info(f"Inserted WebSocket-triggered trade for {order_data['symbol']}: {order_data['order_id']}")
                    if not order_data['is_trailing']:
                        atr = get_current_atr(client, order_data['symbol'])
                        position_type = 'long' if order_data['side'] == 'buy' else 'short'
                        ts_manager.initialize_trailing_stop(
                            symbol=order_data['symbol'],
                            entry_price=order_data['price'],
                            position_type=position_type,
                            quantity=order_data['quantity'],
                            atr=atr
                        )
                        logger.info(f"Initialized trailing stop for WebSocket trade {order_data['order_id']} ({order_data['symbol']})")
                if order_data['is_trailing']:
                    current_price = get_current_price(client, order_data['symbol'])
                    if current_price:
                        ts_manager.update_trailing_stop(order_data['symbol'], current_price)
                        logger.info(f"Updated trailing stop for {order_data['symbol']} at price {current_price}")
    except Exception as e:
        logger.error(f"Error handling WebSocket order update: {str(e)}")

def start_websocket():
    """Démarre le client WebSocket et maintient la connexion."""
    ws_client = UMFuturesWebsocketClient()
    ws_client.user_data(
        listen_key=client.new_listen_key()['listenKey'],
        id=1,
        callback=handle_order_update
    )
    logger.info("[WebSocket] WebSocket client initialized.")
    while True:
        eventlet.sleep(3600)  # Garde la connexion ouverte, rafraîchit le listen_key toutes les heures
        try:
            client.renew_listen_key(ws_client.listen_key)
            logger.info("[WebSocket] Listen key renewed.")
        except Exception as e:
            logger.error(f"[WebSocket] Failed to renew listen key: {e}")

# Lancer le WebSocket dans un thread séparé
websocket_thread = threading.Thread(target=start_websocket, daemon=True)
websocket_thread.start()

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
                candle_data = json.loads(msg.value().decode("utf-8")) if msg.value() else None
                if not candle_data or not isinstance(candle_data, dict):
                    logger.error(f"[Kafka] Invalid candle data for topic {msg.topic()}: {candle_data}")
                    continue
                topic = msg.topic()
                symbol = next((s for s in SYMBOLS if f"{s}_candle" in topic), None)
                if not symbol or symbol not in dataframes:
                    logger.error(f"[Kafka] Invalid symbol for topic {topic}")
                    continue

                candle_df = format_candle(candle_data, symbol)
                if candle_df.empty or "close" not in candle_df.columns:
                    logger.error(f"[Kafka] Invalid or empty candle data for {symbol}: {candle_data}")
                    continue

                # On garde la colonne 'timestamp' pour l'insertion
                insert_price_data(candle_df, symbol)
                # On définit l'index après l'insertion pour la suite du traitement
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
                                    last_order_details[symbol] = order_details[symbol]
                                    record_trade_metric(order_details[symbol])
                                    send_telegram_alert(f"Trade closed: {action.upper()} {symbol} at {price}, PNL: {order_details[symbol]['pnl']}")
                                    current_positions[symbol] = None
                                    last_sl_order_ids[symbol] = None
                            except (TypeError, ValueError, IndexError) as e:
                                logger.error(f"[Order] Error closing {action} order for {symbol}: {e}")
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
        # Start performance tracker in a separate thread
        performance_thread = threading.Thread(target=performance_tracker_loop, args=(client, SYMBOLS), daemon=True)
        performance_thread.start()
        
        # Run main loop
        asyncio.run(main())