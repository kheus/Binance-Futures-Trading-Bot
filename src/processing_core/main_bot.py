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
## from src.processing_core.signal_generator import DataPreprocessor, check_signal,
from src.trade_execution.order_manager import place_order, init_trailing_stop_manager, EnhancedOrderManager
from src.trade_execution.sync_orders import get_current_atr, get_current_adx, sync_binance_trades_with_postgres
from src.database.db_handler import insert_trade, insert_signal, insert_metrics, create_tables, insert_price_data, clean_old_data, update_trade_on_close
from src.processing_core.signal_generator import DataPreprocessor, StrategySelector, check_signal
from src.trade_execution.ultra_aggressive_trailing import TrailingStopManager, get_spread
from src.monitoring.metrics import record_trade_metric
from src.monitoring.alerting import send_telegram_alert
from src.performance.tracker import performance_tracker_loop, evaluate_signals
from src.processing_core.indicators import calculate_indicators
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
import signal
from pathlib import Path
from src.database.db_handler import connection_pool, execute_query

# Initialize rich console for enhanced logging
console = Console()

# Force UTF-8 encoding
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8-sig")
sys.stderr.reconfigure(encoding="utf-8-sig")


# === Dossier de logs ===
os.makedirs("logs", exist_ok=True)

# === Fichier log spécifique à main_bot ===
log_file = "logs/main_bot.log"
file_handler = RotatingFileHandler(log_file, maxBytes=2_000_000, backupCount=5)
file_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(file_formatter)

# === Console avec couleurs (via rich) ===
console_handler = RichHandler(rich_tracebacks=True, markup=True)
console_formatter = logging.Formatter("%(message)s")
console_handler.setFormatter(console_formatter)

# === Configuration globale du logging ===
logging.basicConfig(
    level=logging.DEBUG,  # ← Affiche tout, même DEBUG
    handlers=[file_handler, console_handler],
    encoding="utf-8-sig"
)

# === Logger spécifique à ce module ===
logger = logging.getLogger("main_bot")

# Réduction du bruit de certains modules
logging.getLogger('socketio').setLevel(logging.CRITICAL)
logging.getLogger('werkzeug').setLevel(logging.WARNING)
logging.getLogger('engineio').setLevel(logging.CRITICAL)


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
MODEL_UPDATE_INTERVAL = 600  # 10 minutes pour réentraînement plus fréquent
METRICS_UPDATE_INTERVAL = 300  # 5 minutes
TRAILING_UPDATE_INTERVAL = binance_config.get("trailing_update_interval", 10)  # Default to 10 seconds
PRICE_FETCH_INTERVAL = 60  # Fetch 1-minute candles every 60 seconds
MAX_CONCURRENT_TRADES = 1  # Limite de trades simultanés
MAX_DRAWDOWN = 0.05 * CAPITAL  # 5% drawdown max

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
current_positions = {symbol: None for symbol in SYMBOLS}
try:
    sync_binance_trades_with_postgres(client, SYMBOLS, ts_manager, current_positions)
except psycopg2.pool.PoolError as e:
    logger.error(f"❌ [MainBot] Failed to sync trades due to connection pool exhaustion: {e}")
    send_telegram_alert(f"Failed to sync trades: connection pool exhausted")
    raise
logger.info("[MainBot] Initial trade sync completed. ✅")
logger.debug(f"[MainBot] Current positions after sync: {current_positions}")

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
        logger.debug(f"[WebSocket] Received message: {message}")
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
                'trade_id': order.get('c', str(int(order['T']))).replace('trade_', '')  # Normalize trade_id
            }
            logger.debug(f"[WebSocket] Processing order update for {order_data['symbol']}: order_id={order_data['order_id']}, trade_id={order_data['trade_id']}, status={order_data['status']}")
            if order_data['status'] == 'filled':
                if order_data['is_trailing']:
                    # Trailing stop exécuté
                    symbol = order_data['symbol']
                    trade_id = order_data['trade_id']
                    exit_price = order_data['price']
                    if symbol in ts_manager.stops and ts_manager.stops[symbol].trade_id == trade_id:
                        qty = ts_manager.stops[symbol].quantity
                        entry_price = ts_manager.stops[symbol].entry_price
                        position_type = ts_manager.stops[symbol].position_type
                        leverage = ts_manager.stops[symbol].leverage
                        if position_type == 'long':
                            pnl = (exit_price - entry_price) * qty * leverage
                        else:
                            pnl = (entry_price - exit_price) * qty * leverage
                        connection_pool.execute_query(
                            "UPDATE trades SET status = 'CLOSED', exit_price = %s, realized_pnl = %s, close_timestamp = %s, leverage = %s WHERE trade_id = %s",
                            (exit_price, pnl, int(time.time()), leverage, trade_id)
                        )
                        ts_manager.close_position(symbol)
                        current_positions[symbol] = None
                        logger.info(f"[WebSocket] Trailing stop executed for {symbol}, trade_id={trade_id}, PNL={pnl}")
                    else:
                        logger.warning(f"[WebSocket] Trailing stop mismatch for {symbol}, trade_id={trade_id}")
                else:
                    # Ordre normal (non trailing stop)
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
                    current_positions[order_data['symbol']] = {
                        'side': order_data['side'],
                        'quantity': order_data['quantity'],
                        'price': order_data['price'],
                        'trade_id': order_data['trade_id']
                    }
                    atr = get_current_atr(client, order_data['symbol'])
                    if atr > 0:
                        ts_manager.initialize_trailing_stop(
                            symbol=order_data['symbol'],
                            entry_price=order_data['price'],
                            position_type='long' if order_data['side'] == 'buy' else 'short',
                            quantity=order_data['quantity'],
                            atr=atr,
                            trade_id=order_data['trade_id']
                        )
                    adx = get_current_adx(client, order_data['symbol'])
                    if adx > 0:
                        ts_manager.initialize_trailing_stop(
                            symbol=order_data['symbol'],
                            entry_price=order_data['price'],
                            position_type='long' if order_data['side'] == 'buy' else 'short',
                            quantity=order_data['quantity'],
                            atr=atr,
                            trade_id=order_data['trade_id']
                        )
                        trade_data['is_trailing'] = True
                        insert_trade(trade_data)
                        logger.info(f"[WebSocket] Initialized trailing stop for {order_data['symbol']}, trade_id={order_data['trade_id']}")
                
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
                table.add_row("Trade ID", order_data['trade_id'])
                console.log(table)
            
            elif order_data['status'] in ['canceled', 'rejected', 'expired']:
                if order_data['is_trailing'] and ts_manager.has_trailing_stop(order_data['symbol']):
                    ts_manager.close_position(order_data['symbol'])
                    logger.info(f"[WebSocket] Trailing stop order {order_data['order_id']} for {order_data['symbol']} {order_data['status']}")
            
            # Vérifier si la position est toujours ouverte sur Binance
            positions = client.get_position_risk(symbol=order_data['symbol'])
            position = next((p for p in positions if p['symbol'] == order_data['symbol'] and float(p['positionAmt']) != 0), None)
            if not position and current_positions.get(order_data['symbol']):
                trade_id = current_positions[order_data['symbol']].get('trade_id')
                if trade_id:
                    ticker = client.get_symbol_ticker(symbol=order_data['symbol'])
                    exit_price = float(ticker['price'])
                    qty = float(current_positions[order_data['symbol']]['quantity'])
                    entry_price = float(current_positions[order_data['symbol']]['price'])
                    leverage = current_positions[order_data['symbol']].get('leverage', LEVERAGE)
                    side = current_positions[order_data['symbol']]['side']
                    if side == 'long':
                        pnl = (exit_price - entry_price) * qty * leverage

                    else:
                        pnl = (entry_price - exit_price) * qty * leverage
                    execute_query(
                        "UPDATE trades SET status = 'CLOSED', exit_price = %s, realized_pnl = %s, close_timestamp = %s, leverage = %s WHERE trade_id = %s",
                        (exit_price, pnl, int(time.time()), leverage, trade_id)
                    )
                    ts_manager.close_position(order_data['symbol'])
                    current_positions[order_data['symbol']] = None
                    logger.info(f"[WebSocket] Position closed for {order_data['symbol']}, trade_id={trade_id}, PNL={pnl}")
    except Exception as e:
        logger.error(f"❌ [WebSocket] Error handling order update: {str(e)}")
        send_telegram_alert(f"WebSocket order update error: {str(e)}")

def start_websocket():
    max_retries = 5
    retry_delay = 5
    while True:
        try:
            ws_client = UMFuturesWebsocketClient()
            listen_key = client.new_listen_key()['listenKey']
            logger.info(f"[WebSocket] Listen key: {listen_key}")
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
                    break
        except Exception as e:
            logger.error(f"❌ [WebSocket] Failed to initialize WebSocket: {e}")
            send_telegram_alert(f"WebSocket initialization failed: {str(e)}")
            if max_retries > 0:
                logger.info(f"[WebSocket] Retrying in {retry_delay} seconds... (Attempts left: {max_retries})")
                time.sleep(retry_delay)
                max_retries -= 1
                retry_delay *= 2
            else:
                logger.error("[WebSocket] Max retries reached. WebSocket will not be initialized.")
                send_telegram_alert("WebSocket failed after maximum retries. Please check connectivity.")
                break

async def trailing_stop_updater():
    while True:
        try:
            logger.debug("[Trailing Stop] Updater running, checking positions...")
            logger.debug(f"[Trailing Stop] Current positions: {current_positions}")
            logger.debug(f"[Trailing Stop] Trailing stops: {ts_manager.stops}")
            for symbol in SYMBOLS:
                # Vérifier si la position existe sur Binance
                positions = client.get_position_risk(symbol=symbol)
                position = next((p for p in positions if p['symbol'] == symbol and float(p['positionAmt']) != 0), None)
                trade_id = None
                if current_positions.get(symbol):
                    trade_id = current_positions[symbol].get('trade_id')
                # Position fermée sur Binance
                if current_positions.get(symbol) and not position:
                    if trade_id:
                        ticker = client.get_symbol_ticker(symbol=symbol)
                        exit_price = float(ticker['price'])
                        qty = float(current_positions[symbol]['quantity'])
                        entry_price = float(current_positions[symbol]['price'])
                        leverage = current_positions[symbol].get('leverage', LEVERAGE)
                        side = current_positions[symbol]['side']
                        if side == 'long':
                            pnl = (exit_price - entry_price) * qty * leverage
                        else:
                            pnl = (entry_price - exit_price) * qty * leverage
                        execute_query(
                            "UPDATE trades SET status = 'CLOSED', exit_price = %s, realized_pnl = %s, close_timestamp = %s, leverage = %s WHERE trade_id = %s",
                            (exit_price, pnl, int(time.time()), leverage, trade_id)
                        )
                        ts_manager.close_position(symbol)
                        current_positions[symbol] = None
                        logger.info(f"[Trailing Stop] Position closed for {symbol}, trade_id={trade_id}, PNL={pnl}")
                    continue

                # Position ouverte sur Binance
                if current_positions.get(symbol) and position:
                    if not trade_id:
                        logger.warning(f"[Trailing Stop] No trade_id in current_positions for {symbol}, skipping update")
                        continue
                    trade_id = str(trade_id).replace('trade_', '')  # Normalize trade_id
                    if ts_manager.has_trailing_stop(symbol, trade_id):
                        current_price = order_manager.get_current_price(symbol)
                        if current_price:
                            atr = get_current_atr(client, symbol)
                            spread = get_spread(client, symbol)
                            adx = get_current_adx(client, symbol)
                            ts_manager.update_trailing_stop(symbol, current_price, atr, spread, adx, trade_id=trade_id)
                            if ts_manager.has_trailing_stop(symbol, trade_id):
                                table = Table(title=f"Trailing Stop Update for {symbol}")
                                table.add_column("Field", style="cyan")
                                table.add_column("Value", style="magenta")
                                table.add_row("Symbol", symbol)
                                table.add_row("Current Price", f"{current_price:.4f}")
                                table.add_row("Stop Price", f"{ts_manager.stops[symbol].stop_loss_price:.4f}")
                                table.add_row("Trade ID", trade_id)
                                console.log(table)
                                logger.info(f"[Trailing Stop] Updated trailing stop for {symbol} at price {current_price:.4f}, trade_id={trade_id} 🔄")
                        else:
                            logger.warning(f"[Trailing Stop] Failed to get current price for {symbol}")
                    else:
                        logger.warning(f"[Trailing Stop] No active trailing stop for {symbol}, trade_id={trade_id}")
                elif trade_id and ts_manager.has_trailing_stop(symbol, trade_id):
                    logger.warning(f"[Trailing Stop] Inconsistent state: trailing stop exists for {symbol}, trade_id={trade_id} but no position in current_positions")
                    ts_manager.close_position(symbol)
            await asyncio.sleep(TRAILING_UPDATE_INTERVAL)
        except Exception as e:
            logger.error(f"❌ [Trailing Stop] Error in trailing stop updater: {e}")
            send_telegram_alert(f"Error in trailing stop updater: {str(e)}")
            await asyncio.sleep(TRAILING_UPDATE_INTERVAL)

async def fetch_price_data_fallback():
    while True:
        try:
            for symbol in SYMBOLS:
                try:
                    klines = client.klines(symbol=symbol, interval='1m', limit=5)
                    if not klines:
                        logger.warning(f"[Price Fetch] No 1m klines fetched for {symbol}")
                        continue
                    candle_df = pd.DataFrame(
                        klines,
                        columns=["open_time", "open", "high", "low", "close", "volume",
                                 "close_time", "quote_asset_vol", "num_trades", "taker_buy_base_vol",
                                 "taker_buy_quote_vol", "ignore"]
                    )
                    candle_df = candle_df[["open_time", "open", "high", "low", "close", "volume"]].astype(float)
                    candle_df["open_time"] = pd.to_datetime(candle_df["open_time"], unit="ms")
                    candle_df.set_index("open_time", inplace=True)
                    if insert_price_data(candle_df, symbol):
                        logger.debug(f"[Price Fetch] Inserted 1m price data for {symbol} at {int(candle_df.index[-1].timestamp() * 1000)}")
                    else:
                        logger.error(f"[Price Fetch] Failed to insert 1m price data for {symbol}")
                except Exception as e:
                    logger.error(f"[Price Fetch] Error fetching 1m klines for {symbol}: {e}")
            await asyncio.sleep(PRICE_FETCH_INTERVAL)
        except Exception as e:
            logger.error(f"[Price Fetch] Error in fallback price fetcher: {e}")
            await asyncio.sleep(PRICE_FETCH_INTERVAL)

async def main():
    dataframes = {symbol: pd.DataFrame() for symbol in SYMBOLS}
    order_details = {symbol: None for symbol in SYMBOLS}
    last_order_details = {symbol: None for symbol in SYMBOLS}
    last_model_updates = {symbol: time.time() for symbol in SYMBOLS}
    last_action_sent = {symbol: (None, 0) for symbol in SYMBOLS}
    last_sl_order_ids = {symbol: None for symbol in SYMBOLS}
    models = {symbol: None for symbol in SYMBOLS}
    scalers = {symbol: None for symbol in SYMBOLS}
    last_sync_time = time.time()
    sync_interval = 120  # 2 minutes
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
        data_hist["timestamp"] = pd.to_datetime(data_hist["open_time"], unit="ms")
        data_hist.set_index("timestamp", inplace=True)
        dataframes[symbol] = data_hist.tail(500)
        dataframes[symbol] = calculate_indicators(dataframes[symbol], symbol)
        required_cols = ['close', 'volume', 'RSI', 'MACD', 'ADX']
        if dataframes[symbol] is not None and not dataframes[symbol].empty and all(col in dataframes[symbol].columns for col in required_cols):
            logger.info(f"[Main] Indicators calculated successfully for {symbol}")
        else:
            logger.error(f"[Main] Failed to calculate indicators for {symbol}, DataFrame: {dataframes[symbol].columns.tolist() if dataframes[symbol] is not None else 'None'}")

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
    time.sleep(10)

    last_log_time = time.time()
    iteration_count = 0
    try:
        # Start fallback price data fetcher
        asyncio.create_task(fetch_price_data_fallback())
        logger.info("[MainBot] Backfilling historical price data for evaluation windows...")
        for symbol in SYMBOLS:
            historical_candles = client.klines(symbol, '1m', limit=100)  # ~1.6h d'historique
            for candle in historical_candles:
                candle_df = format_candle(candle, symbol)
                if not candle_df.empty:
                    insert_price_data(candle_df, symbol)
        logger.info("[MainBot] Historical backfill completed.")
        while True:
            current_time = time.time()
            if current_time - last_sync_time >= sync_interval:
                logger.info("[MainBot] Running periodic trade sync... 🚀")
                try:
                    sync_binance_trades_with_postgres(client, SYMBOLS, ts_manager, current_positions)
                    order_manager.clean_orphaned_trailing_stops(ts_manager)  # Nettoyer les trailing stops orphelins
                    logger.debug(f"[MainBot] Current positions after sync: {current_positions}")
                except psycopg2.pool.PoolError as e:
                    logger.error(f"❌ [MainBot] Failed to sync trades due to connection pool exhaustion: {e}")
                    send_telegram_alert(f"Failed to sync trades: connection pool exhausted")
                    # Réessayer après un délai
                    await asyncio.sleep(60)
                    continue
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
                            lstm_input = DataPreprocessor.prepare_lstm_input(dataframes[symbol])
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

                if len(dataframes[symbol]) >= 50:
                    dataframes[symbol] = calculate_indicators(dataframes[symbol], symbol)
                    last_metrics_update[symbol] = current_time

                if len(dataframes[symbol]) >= 101:
                    logger.debug(f"[main_bot] Calling check_signal for {symbol} with dataframe length {len(dataframes[symbol])}")
                    logger.debug(f"[main_bot] Current positions before signal: {current_positions[symbol]}")
                    action, new_position, confidence, confidence_factors = check_signal(
                        df=dataframes[symbol],
                        model=models[symbol],
                        current_position=current_positions.get(symbol),
                        last_order_details=last_order_details.get(symbol),
                        symbol=symbol,
                        last_action_sent=last_action_sent.get(symbol) if isinstance(last_action_sent.get(symbol), tuple) else None,
                        config=config
                    )

                    if action in ("buy", "sell", "close_buy", "close_sell") and action == last_action_sent.get(symbol, (None,))[0]:
                        logger.info(f"[Signal] Ignored repeated action for {symbol}: {action} 🔄")
                        continue
                    timestamp = int(candle_df.index[-1].timestamp() * 1000)
                    last_action_sent[symbol] = (action, timestamp)

                    if action in ["buy", "sell"]:
                        try:
                            price = float(candle_df["close"].iloc[-1])
                            adx = get_current_adx(client, symbol)
                            atr = get_current_atr(client, symbol)
                            if atr <= 0:
                                atr = price * 0.005  # Fallback to 0.5% of price as min ATR
                                logger.warning(f"⚠️ [Order] Using fallback ATR {atr} for {symbol}")
                                continue
                            order_details[symbol] = order_manager.place_enhanced_order(action, symbol, CAPITAL, LEVERAGE, trade_id=str(timestamp))
                            if order_details[symbol]:
                                logger.debug(f"[main_bot] New position from signal: {new_position}")
                                logger.debug(f"[main_bot] Order details: {order_details[symbol]}")
                                current_positions[symbol] = {
                                    'side': 'long' if action == 'buy' else 'short',
                                    'quantity': order_details[symbol]['quantity'],
                                    'price': order_details[symbol]['price'],
                                    'trade_id': order_details[symbol]['trade_id']
                                }
                                logger.debug(f"[main_bot] Updated current_positions[{symbol}] = {current_positions[symbol]}")
                                try:
                                    insert_trade(order_details[symbol])
                                    logger.info(f"[Main] Trade inserted for {symbol}: {order_details[symbol]['order_id']}")
                                except Exception as e:
                                    logger.error(f"❌ [Main] Failed to insert trade for {symbol}: {e}")
                                    send_telegram_alert(f"Failed to insert trade for {symbol}: {str(e)}")
                                last_order_details[symbol] = order_details[symbol]
                                record_trade_metric(order_details[symbol])
                                send_telegram_alert(f"Trade executed: {action.upper()} {symbol} at {price} 💰")
                                try:
                                    last_sl_order_ids[symbol] = ts_manager.initialize_trailing_stop(
                                        symbol=symbol,
                                        entry_price=order_details[symbol]['price'],
                                        position_type='long' if action == 'buy' else 'short',
                                        quantity=order_details[symbol]['quantity'],
                                        atr=atr,
                                        adx=adx,
                                        trade_id=order_details[symbol]['trade_id']
                                    )
                                    if last_sl_order_ids[symbol]:
                                        order_details[symbol]['is_trailing'] = True
                                        try:
                                            insert_trade(order_details[symbol])
                                            logger.info(f"[Main] Trade updated with trailing stop for {symbol}: {order_details[symbol]['order_id']}")
                                        except Exception as e:
                                            logger.error(f"❌ [Main] Failed to update trade with trailing stop for {symbol}: {e}")
                                            send_telegram_alert(f"Failed to update trade with trailing stop for {symbol}: {str(e)}")
                                        logger.info(f"[Main] Trailing stop initialized for {symbol}, trade_id: {order_details[symbol]['trade_id']}")
                                    else:
                                        logger.error(f"❌ [Main] Failed to initialize trailing stop for {symbol}")
                                        send_telegram_alert(f"Failed to initialize trailing stop for {symbol}")
                                except Exception as e:
                                    logger.error(f"❌ [Main] Error initializing trailing stop for {symbol}: {e}")
                                    send_telegram_alert(f"Error initializing trailing stop for {symbol}: {str(e)}")
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
                                    leverage = current_positions[symbol].get('leverage', LEVERAGE)
                                    if side == "buy":
                                        pnl = (close_price - open_price) * qty * leverage
                                    else:
                                        pnl = (open_price - close_price) * qty * leverage
                                    order_details[symbol]["pnl"] = pnl
                                    trade_id = last_order_details[symbol].get("trade_id")
                                    if trade_id:
                                        execute_query(
                                            "UPDATE trades SET status = 'CLOSED', exit_price = %s, realized_pnl = %s, close_timestamp = %s, leverage = %s WHERE trade_id = %s",
                                            (close_price, pnl, int(time.time()), leverage, trade_id)
                                        )
                                        ts_manager.close_position(symbol)
                                        current_positions[symbol] = None
                                        logger.info(f"[Main] Position closed for {symbol}, trade_id={trade_id}, PNL={pnl}")
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
                            else:
                                logger.error(f"❌ [Order] Failed to place {close_side} order for {symbol}")
                        except (TypeError, ValueError, IndexError, psycopg2.pool.PoolError) as e:
                            logger.error(f"❌ [Order] Error closing {action} order for {symbol}: {e}")
                            send_telegram_alert(f"Error closing {action} order for {symbol}: {str(e)}")
            except Exception as e:
                logger.error(f"❌ [Kafka] Exception while processing candle for {symbol}: {e}")
                continue

            # === Evaluation des signaux déjà générés ===
            try:
                evaluate_signals(connection_pool)
            except Exception as e:
                logger.error(f"[Main] Signal evaluation error: {e}")

            await asyncio.sleep(60)  # Évalue toutes les minutes

    except Exception as e:
        logger.error(f"[Main] Loop error: {e}")
        send_telegram_alert(f"Main loop error: {str(e)}")
        await asyncio.sleep(5)

def tracker_loop():
    last_cleanup_date = None
    while True:
        try:
            now = datetime.now()
            if now.hour == 0 and (last_cleanup_date is None or last_cleanup_date != now.date()):
                clean_old_data()
                last_cleanup_date = now.date()
            time.sleep(60)
        except Exception as e:
            logger.error(f"[Tracker] Error in tracker loop: {e}", exc_info=True)

# ---------------------------
# MAIN RUNNER & SHUTDOWN HANDLER
# ---------------------------

from typing import List

_GLOBAL_ASYNC_TASKS: List[asyncio.Task] = []

def start_background_threads():
    """Start non-async threads (websocket, performance tracker)."""
    performance_thread = threading.Thread(target=performance_tracker_loop, args=(client, SYMBOLS), daemon=True)
    performance_thread.start()
    websocket_thread = threading.Thread(target=start_websocket, daemon=True)
    websocket_thread.start()
    logger.info("[Main] Background threads started (websocket & performance tracker).")

async def _create_and_register_task(coro):
    """Create task and register it in global list for later cancellation."""
    task = asyncio.create_task(coro)
    _GLOBAL_ASYNC_TASKS.append(task)
    return task

async def _shutdown(loop, reason="shutdown"):
    """Cancel and await all tasks, close consumer and loop resources."""
    logger.info(f"[Shutdown] Initiated ({reason}). Cancelling tasks...")
    # Cancel our registered tasks
    for t in list(_GLOBAL_ASYNC_TASKS):
        if not t.done():
            t.cancel()
    # Also cancel any other tasks except current
    current = asyncio.current_task()
    others = [t for t in asyncio.all_tasks(loop) if t is not current]
    for t in others:
        if not t.done():
            t.cancel()

    # Wait for tasks to finish
    await asyncio.gather(*others, return_exceptions=True)
    # Close Kafka consumer
    try:
        logger.info("[Shutdown] Closing Kafka consumer...")
        consumer.close()
    except Exception as e:
        logger.warning(f"[Shutdown] Error closing consumer: {e}")

    # Close DB pools or connection_pool if needed
    try:
        if hasattr(connection_pool, "closeall"):
            connection_pool.closeall()
            logger.info("[Shutdown] DB connection pool closed.")
    except Exception:
        logger.debug("[Shutdown] No connection_pool.closeall() available or error on close.", exc_info=True)

    # Shutdown async generators
    await loop.shutdown_asyncgens()
    logger.info("[Shutdown] All async generators shut down.")

def _handle_signal(sig, frame):
    """Synchronous signal handler that schedules the async shutdown."""
    logger.info(f"[Signal] Received signal {sig}. Scheduling shutdown...")
    loop = asyncio.get_event_loop()
    asyncio.run_coroutine_threadsafe(_shutdown(loop, reason=f"signal_{sig}"), loop)

def run_bot():
    """Entrypoint to run the bot with graceful shutdown handling."""
    # Start thread-based services first
    start_background_threads()

    # Create an asyncio event loop and register tasks
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # Register signal handlers for graceful shutdown (works on Unix; Windows limited)
    try:
        for s in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(s, lambda sig=s: _handle_signal(sig, None))
    except NotImplementedError:
        # Some platforms (Windows) may not implement add_signal_handler for the loop
        signal.signal(signal.SIGINT, lambda s, f: _handle_signal(s, f))
        signal.signal(signal.SIGTERM, lambda s, f: _handle_signal(s, f))

    try:
        # Create and keep references to long-running tasks
        loop.run_until_complete(_create_and_register_task(trailing_stop_updater()))
        loop.run_until_complete(_create_and_register_task(fetch_price_data_fallback()))
        # Run main; main itself is an endless loop until cancelled
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        logger.info("[Main] KeyboardInterrupt caught. Running shutdown...")
        loop.run_until_complete(_shutdown(loop, reason="keyboard_interrupt"))
    except Exception as e:
        logger.exception(f"[Main] Unexpected exception: {e}")
        loop.run_until_complete(_shutdown(loop, reason="exception"))
    finally:
        # Ensure shutdown
        try:
            if not loop.is_closed():
                loop.run_until_complete(_shutdown(loop, reason="finalizing"))
        except Exception:
            logger.exception("[Main] Error during final shutdown.")
        finally:
            loop.close()
            logger.info("[Main] Event loop closed. Bot stopped ✅")

if __name__ == "__main__":
    run_bot()