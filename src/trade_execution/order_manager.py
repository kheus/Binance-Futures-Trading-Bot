try:
    from src.trade_execution.order_utils import get_open_orders   
    from binance.enums import SIDE_BUY, SIDE_SELL, ORDER_TYPE_MARKET
    from src.trade_execution.correlation_monitor import CorrelationMonitor
    from src.trade_execution.trade_analyzer import TradeAnalyzer
    from src.monitoring.metrics import get_current_atr
    from src.trade_execution.ultra_aggressive_trailing import TrailingStopManager, get_average_fill_price
except ImportError:
    SIDE_BUY = 'BUY'
    SIDE_SELL = 'SELL'
    ORDER_TYPE_MARKET = 'MARKET'

import time
import logging
import yaml
import talib
import numpy as np
import pandas as pd
from pathlib import Path
from tabulate import tabulate
import inspect
from binance.um_futures import UMFutures
from src.monitoring.alerting import send_telegram_alert
import psycopg2
from psycopg2 import pool
import math
import asyncio

logger = logging.getLogger(__name__)

# Load global configuration
CONFIG_PATH = Path(__file__).parent.parent.parent / "config" / "config.yaml"
with open(CONFIG_PATH, "r", encoding="utf-8-sig") as f:
    config = yaml.safe_load(f)

# Retrieve API keys and symbols
BINANCE_API_KEY = config["binance"]["api_key"]
BINANCE_API_SECRET = config["binance"]["api_secret"]
SYMBOLS = config["binance"]["symbols"]

# Import market crash protector
from src.trade_execution.market_crash_protector import MarketCrashProtector

# Initialize global trailing stop manager and crash protector
ts_manager = None
crash_protector = MarketCrashProtector()

# Database configuration
CONFIG_PATH = Path(__file__).parent.parent.parent / "config" / "db_config.yaml"
with open(CONFIG_PATH, "r", encoding="utf-8-sig") as f:
    db_config = yaml.safe_load(f)

DB_CONFIG = db_config["database"]["postgresql"]
HOST = DB_CONFIG["host"]
DBNAME = DB_CONFIG["database"]
USER = DB_CONFIG["user"]
PASSWORD = DB_CONFIG["password"]
PORT = DB_CONFIG.get("port", 5432)

# Initialize connection pool
try:
    connection_pool = psycopg2.pool.SimpleConnectionPool(
        minconn=1,
        maxconn=10,
        host=HOST,
        dbname=DBNAME,
        user=USER,
        password=PASSWORD,
        port=PORT
    )
    logger.info("[db_handler] Database connection pool initialized.")
except Exception as e:
    logger.error(f"[db_handler] Failed to initialize connection pool: {e}")
    raise

def init_trailing_stop_manager(client):
    global ts_manager
    ts_manager = TrailingStopManager(client)
    # Load existing trailing stops from trades table
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        query = """
        SELECT trade_id, symbol, side, quantity, price, stop_loss
        FROM trades
        WHERE is_trailing = TRUE AND status = 'OPEN'
        """
        cursor.execute(query)
        trades = cursor.fetchall()
        for trade in trades:
            trade_id, symbol, side, quantity, entry_price, stop_loss = trade
            position_type = 'long' if side.upper() == 'BUY' else 'short'
            ts_id = ts_manager.initialize_trailing_stop(
                symbol=symbol,
                entry_price=float(entry_price),
                position_type=position_type,
                quantity=float(quantity),
                atr=get_current_atr(client, symbol),
                trade_id=str(trade_id)
            )
            if ts_id:
                logger.info(f"[TrailingStopManager] Restored trailing stop for {symbol}, trade_id={trade_id}, stop_loss={stop_loss}")
            else:
                logger.error(f"[TrailingStopManager] Failed to restore trailing stop for {symbol}, trade_id={trade_id}")
    except Exception as e:
        logger.error(f"[TrailingStopManager] Error restoring trailing stops: {e}")
    finally:
        if conn:
            release_db_connection(conn)
    return ts_manager

async def trailing_stop_monitor(client):
    """Monitor and update trailing stops for all open trades."""
    while True:
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            query = """
            SELECT trade_id, symbol, side, quantity, price, stop_loss
            FROM trades
            WHERE is_trailing = TRUE AND status = 'OPEN'
            """
            cursor.execute(query)
            trades = cursor.fetchall()
            for trade in trades:
                trade_id, symbol, side, quantity, entry_price, stop_loss = trade
                try:
                    # Use synchronous client for price fetch if async not available
                    ticker = client.ticker_price(symbol=symbol)
                    current_price = float(ticker['price'])
                    position_type = 'long' if side.upper() == 'BUY' else 'short'
                    ts_id = ts_manager.update_trailing_stop(symbol, current_price, trade_id=str(trade_id))
                    if ts_id:
                        # Update stop_loss in trades table
                        new_stop_loss = ts_manager.get_current_stop_price(symbol, trade_id=str(trade_id))
                        if new_stop_loss:
                            update_query = """
                            UPDATE trades
                            SET stop_loss = %s, timestamp = %s
                            WHERE trade_id = %s
                            """
                            cursor.execute(update_query, (new_stop_loss, int(time.time() * 1000), trade_id))
                            conn.commit()
                            logger.info(f"[TrailingStopMonitor] Updated trailing stop for {symbol}, trade_id={trade_id}, new_stop_loss={new_stop_loss}")
                        # Check if stop loss triggered
                        if position_type == 'long' and current_price <= new_stop_loss:
                            order = client.create_order(
                                symbol=symbol,
                                side=SIDE_SELL,
                                type=ORDER_TYPE_MARKET,
                                quantity=quantity
                            )
                            update_query = """
                            UPDATE trades
                            SET status = 'CLOSED', exit_price = %s, timestamp = %s
                            WHERE trade_id = %s
                            """
                            cursor.execute(update_query, (current_price, int(time.time() * 1000), trade_id))
                            conn.commit()
                            logger.info(f"[TrailingStopMonitor] Stop loss triggered for {symbol}, trade_id={trade_id} at {current_price}")
                            ts_manager.remove_trailing_stop(symbol, trade_id=str(trade_id))
                        elif position_type == 'short' and current_price >= new_stop_loss:
                            order = client.create_order(
                                symbol=symbol,
                                side=SIDE_BUY,
                                type=ORDER_TYPE_MARKET,
                                quantity=quantity
                            )
                            update_query = """
                            UPDATE trades
                            SET status = 'CLOSED', exit_price = %s, timestamp = %s
                            WHERE trade_id = %s
                            """
                            cursor.execute(update_query, (current_price, int(time.time() * 1000), trade_id))
                            conn.commit()
                            logger.info(f"[TrailingStopMonitor] Stop loss triggered for {symbol}, trade_id={trade_id} at {current_price}")
                            ts_manager.remove_trailing_stop(symbol, trade_id=str(trade_id))
                except Exception as e:
                    logger.error(f"[TrailingStopMonitor] Error updating trailing stop for {symbol}, trade_id={trade_id}: {e}")
            await asyncio.sleep(30)  # Check every 30 seconds
        except Exception as e:
            logger.error(f"[TrailingStopMonitor] Error in monitor loop: {e}")
            await asyncio.sleep(60)
        finally:
            if conn:
                release_db_connection(conn)

def get_tick_info(client, symbol):
    try:
        exchange_info = client.get_exchange_info()
        symbol_info = next((s for s in exchange_info["symbols"] if s["symbol"] == symbol), None)
        if not symbol_info:
            logger.error(f"[OrderManager] Symbol {symbol} not found in exchange info.")
            return 0.0001, 8  # Fallback values

        # Find PRICE_FILTER explicitly
        price_filter = next((f for f in symbol_info['filters'] if f['filterType'] == 'PRICE_FILTER'), None)
        if not price_filter:
            logger.error(f"[OrderManager] PRICE_FILTER not found for {symbol}. Using fallback tick size.")
            return 0.0001, 8  # Fallback values

        # Find LOT_SIZE filter for quantity precision
        lot_size_filter = next((f for f in symbol_info['filters'] if f['filterType'] == 'LOT_SIZE'), None)
        if not lot_size_filter:
            logger.error(f"[OrderManager] LOT_SIZE filter not found for {symbol}. Using fallback precision.")
            return 0.0001, 8  # Fallback values

        tick_size = float(price_filter['tickSize'])
        quantity_precision = int(symbol_info.get('quantityPrecision', 8))  # Use exchange-provided precision or default
        return tick_size, quantity_precision
    except Exception as e:
        logger.error(f"[OrderManager] Error getting tick info for {symbol}: {e}")
        return 0.0001, 8  # Fallback values

def round_to_tick(value, tick_size):
    return round(round(value / tick_size) * tick_size, 8)

def check_open_position(client, symbol, side, current_positions):
    """
    Vérifie si une position est ouverte avec une gestion robuste des erreurs.
    Retourne (has_position: bool, position_qty: float)
    """
    position_qty = 0.0
    has_position = False

    try:
        # Vérification via le tracker interne
        current_position = current_positions.get(symbol)
        if isinstance(current_position, dict):
            position_qty = float(current_position.get('quantity', 0.0))
            has_position = position_qty != 0.0
            logger.debug(f"[Position Check] Using dict position for {symbol}: qty={position_qty}")
        elif current_position is not None:
            logger.warning(f"[Position Check] Invalid position format for {symbol}: {current_position}")

        # Vérification via l'API Binance
        try:
            position_info = client.get_position_risk(symbol=symbol)
            for pos in position_info:
                if pos['symbol'] == symbol:
                    qty = float(pos['positionAmt'])
                    if qty != 0:
                        api_position_side = 'long' if qty > 0 else 'short'
                        has_position = True
                        position_qty = abs(qty)
                        # Log en cas de divergence entre tracker et API
                        if isinstance(current_position, dict) and current_position.get('side') != api_position_side:
                            logger.warning(f"[Position Mismatch] {symbol}: tracker={current_position.get('side')}, API={api_position_side}")
                        break
        except Exception as api_error:
            logger.warning(f"[API Position Error] For {symbol}, using tracker value: {api_error}")

        logger.debug(f"[Position Result] {symbol}: has_position={has_position}, qty={position_qty}")
        return has_position, position_qty

    except Exception as e:
        logger.error(f"[Position Check Error] For {symbol}: {str(e)}")
        return False, 0.0  # Fallback safe

def get_exchange_precision(client, symbol):
    """Récupère les règles de précision exactes depuis l'API Binance"""
    try:
        info = client.exchange_info()
        symbol_info = next((s for s in info['symbols'] if s['symbol'] == symbol), None)
        if not symbol_info:
            raise ValueError(f"Symbol {symbol} not found")
        filters = {f['filterType']: f for f in symbol_info['filters']}
        return {
            'price_tick': float(filters['PRICE_FILTER']['tickSize']),
            'qty_step': float(filters['LOT_SIZE']['stepSize']),
            'min_qty': float(filters['LOT_SIZE']['minQty']),
            'price_precision': symbol_info['pricePrecision'],
            'qty_precision': symbol_info['quantityPrecision']
        }
    except Exception as e:
        logger.error(f"Error getting precision for {symbol}: {e}")
        # Valeurs par défaut pour XRP/USDT
        return {
            'price_tick': 0.0001,
            'qty_step': 1.0,
            'min_qty': 1.0,
            'price_precision': 4,
            'qty_precision': 0
        }

def place_order(signal, price, atr, client, symbol, capital, leverage, trade_id=None):
    try:
        rules = get_exchange_precision(client, symbol)
        logger.info(f"Precision rules for {symbol}: {rules}")

        price = round(price / rules['price_tick']) * rules['price_tick']
        price = round(price, rules['price_precision'])

        qty = (capital * leverage) / price
        qty = math.floor(qty / rules['qty_step']) * rules['qty_step']
        qty = max(qty, rules['min_qty'])
        qty = round(qty, rules['qty_precision'])

        if symbol == 'XRPUSDT':
            qty = int(qty)

        logger.info(f"""
Order details for {symbol}:
Original Price: {price}
Rounded Price: {round(price / rules['price_tick']) * rules['price_tick']}
Capital: {capital}
Leverage: {leverage}
Raw Qty: {(capital * leverage) / price}
Adjusted Qty: {qty}
Rules: {rules}
""")

        order = client.create_order(
            symbol=symbol,
            side=SIDE_SELL if signal == 'sell' else SIDE_BUY,
            type=ORDER_TYPE_MARKET,
            quantity=qty,
            recvWindow=10000
        )

        order_id = order.get('orderId') or order.get('clientOrderId')
        if not order_id:
            raise ValueError("Invalid order response from Binance")

        order_status = wait_until_order_finalized(client, symbol, order_id)

        if isinstance(order_status, dict) and order_status.get("status") == "REJECTED":
            logger.error(f"[OrderManager] Order rejected: {order_status.get('msg', 'Unknown reason')}")
            return None

        if isinstance(order_status, dict) and order_status.get("status") == "PARTIALLY_FILLED":
            logger.warning(f"[OrderManager] Partial fill: {order_status['executedQty']}/{order_status['origQty']}")
            qty = float(order_status["executedQty"])

        avg_price = get_average_fill_price(client, symbol, order_id) or price
        position_type = "long" if signal == "buy" else "short"

        # Store trade in database
        trade_id = trade_id or str(int(time.time()))
        conn = get_db_connection()
        cursor = conn.cursor()
        query = """
        INSERT INTO trades (order_id, symbol, side, quantity, price, stop_loss, is_trailing, trade_id, status, timestamp)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (trade_id) DO UPDATE
        SET order_id = EXCLUDED.order_id,
            quantity = EXCLUDED.quantity,
            price = EXCLUDED.price,
            stop_loss = EXCLUDED.stop_loss,
            is_trailing = EXCLUDED.is_trailing,
            status = EXCLUDED.status,
            timestamp = EXCLUDED.timestamp
        """
        cursor.execute(query, (
            str(order_id), symbol, signal.upper(), qty, avg_price, None, False, trade_id, 'OPEN', int(time.time() * 1000)
        ))
        conn.commit()
        release_db_connection(conn)

        # Initialize trailing stop
        ts_id = None
        stop_loss = None
        has_position, position_qty = check_open_position(client, symbol, signal)
        if has_position:
            ts_id = ts_manager.initialize_trailing_stop(
                symbol=symbol,
                entry_price=avg_price,
                position_type=position_type,
                quantity=qty,
                atr=atr,
                trade_id=trade_id
            )
            if ts_id:
                # Update trades table with trailing stop details
                conn = get_db_connection()
                cursor = conn.cursor()
                stop_loss = ts_manager.get_current_stop_price(symbol, trade_id=trade_id)
                cursor.execute(
                    "UPDATE trades SET is_trailing = %s, stop_loss = %s, status = %s WHERE trade_id = %s",
                    (True, stop_loss, 'OPEN', trade_id)
                )
                conn.commit()
                release_db_connection(conn)
                logger.info(f"[OrderManager] Initialized trailing stop for {symbol}, trade_id={trade_id}, stop_loss={stop_loss}")
            else:
                logger.error(f"[OrderManager] Trailing stop init failed for {symbol}, trade_id={trade_id}. Canceling order.")
                try:
                    client.cancel_order(symbol=symbol, orderId=order_id)
                    conn = get_db_connection()
                    cursor = conn.cursor()
                    cursor.execute("UPDATE trades SET status = 'CANCELED' WHERE trade_id = %s", (trade_id,))
                    conn.commit()
                    release_db_connection(conn)
                except Exception as e:
                    logger.error(f"[OrderManager] Failed to cancel order {order_id} for {symbol}: {e}")
                return None

        order_details = {
            "order_id": str(order_id),
            "symbol": symbol,
            "side": signal,
            "quantity": qty,
            "price": avg_price,
            "timestamp": int(time.time() * 1000),
            "is_trailing": bool(ts_id),
            "stop_loss": stop_loss if ts_id else None,
            "take_profit": None,
            "pnl": 0.0,
            "trade_id": trade_id
        }
        logger.info(f"[OrderManager] Successfully placed {signal} order for {symbol}: {order_details}")
        send_telegram_alert(f"Order placed for {symbol} - {signal.upper()} at {avg_price:.2f} USDT, Qty: {qty:.4f} with ATR: {atr:.2f}")
        log_order_as_table(signal, symbol, avg_price, atr, qty, order_details)
        track_order_locally(order_details)
        return order_details

    except Exception as e:
        logger.error(f"Échec placement ordre: {str(e)}")
        send_telegram_alert(f"Order placement failed for {symbol}: {str(e)}")
        logger.debug(f"Full Binance response: {locals().get('order', {})}")
        import traceback
        logger.error(traceback.format_exc())
        return None

def update_trailing_stop(client, symbol, signal, current_price, atr, base_qty, existing_sl_order_id, trade_id=None):
    global ts_manager

    if ts_manager is None:
        ts_manager = init_trailing_stop_manager(client)

    try:
        # Vérifier si une position est ouverte
        has_position, position_qty = check_open_position(client, symbol, signal)
        if not has_position:
            logger.info(f"[Trailing Stop] No open position for {symbol} on side {signal}. Skipping trailing stop update.")
            return None

        # Normalize trade_id
        trade_id = str(trade_id or existing_sl_order_id).replace('trade_', '')

        # Vérifier si un trailing stop existe déjà
        open_orders = client.get_open_orders(symbol=symbol)
        for order in open_orders:
            if order['clientOrderId'].startswith(f"trailing_stop_{symbol}_{trade_id}"):
                logger.info(f"[Trailing Stop] Trailing stop already exists for {symbol} trade {trade_id}. Updating.")
                return ts_manager.update_trailing_stop(symbol, current_price, trade_id=trade_id)

        # Initialiser un nouveau trailing stop si aucun n'existe
        if existing_sl_order_id in (-1, None):
            position_type = "long" if signal == "buy" else "short"
            return ts_manager.initialize_trailing_stop(
                symbol=symbol,
                entry_price=current_price,
                position_type=position_type,
                quantity=base_qty,
                atr=atr,
                trade_id=trade_id or str(int(time.time()))
            )
        return ts_manager.update_trailing_stop(symbol, current_price, trade_id=trade_id)
    except Exception as e:
        logger.error(f"[OrderManager] Error updating trailing stop for {symbol}: {e}")
        return None

def place_scaled_take_profits(client, symbol, entry_price, quantity, position_type, atr, ts_manager, trade_id=None):
    try:
        tp_levels = [
            {"factor": 1.5, "percent": 0.4, "reduce_ts": True},
            {"factor": 3.0, "percent": 0.4, "reduce_ts": True},
            {"factor": 5.0, "percent": 0.2, "close_position": True}
        ]
        trade_id = str(trade_id or int(time.time())).replace('trade_', '')
        for level in tp_levels:
            tp_price = entry_price + (level["factor"] * atr) if position_type == "long" else entry_price - (level["factor"] * atr)
            partial_qty = quantity * level["percent"]
            client.create_order(
                symbol=symbol,
                side=SIDE_SELL if position_type == "long" else SIDE_BUY,
                type="TAKE_PROFIT_MARKET",
                stopPrice=str(tp_price),
                quantity=partial_qty,
                closePosition=level.get("close_position", False),
                priceProtect=True,
                newClientOrderId=f"tp_{symbol}_{trade_id}"
            )
            if level.get("reduce_ts"):
                ts_manager.adjust_quantity(symbol, quantity * (1 - level["percent"]), trade_id=trade_id)
        logger.info(f"[OrderManager] Placed scaled take-profits for {symbol}, trade_id={trade_id}")
    except Exception as e:
        logger.error(f"[OrderManager] Error placing take-profits for {symbol}: {e}")

def log_order_as_table(signal, symbol, price, atr, qty, order_result=None):
    table_data = [
        ["Timestamp", int(time.time() * 1000)],
        ["Symbol", symbol],
        ["Signal", signal],
        ["Price", f"{price:.2f}"],
        ["ATR", f"{atr:.2f}"],
        ["Quantity", f"{qty:.2f}"],
        ["Status", order_result.get("status", "PENDING") if order_result else "FAILED"]
    ]
    logger.info("Order Details:\n%s", tabulate(table_data, headers=["Metric", "Value"], tablefmt="grid"))

order_tracking = {}

def track_order_locally(order):
    try:
        order_id = order.get("order_id") or order.get("orderId") or order.get("clientOrderId") or str(int(time.time() * 1000))
        order_tracking[order_id] = {
            "symbol": order["symbol"],
            "status": order.get("status", "UNKNOWN"),
            "price": order.get("avgPrice") or order.get("price"),
            "timestamp": int(time.time() * 1000),
            "trade_id": order.get("trade_id")
        }
        logger.info(f"[OrderManager] Locally tracked order: {order_tracking[order_id]}")
    except KeyError as e:
        logger.error(f"Invalid order format - missing key {e}")

class EnhancedOrderManager:
    def __init__(self, client: UMFutures, symbols):
        self.client = client
        self.symbols = symbols

    def get_current_price(self, symbol):
        try:
            ticker = self.client.ticker_price(symbol=symbol)
            price = float(ticker['price'])
            logger.debug(f"[OrderManager] Fetched price for {symbol}: {price}")
            return price
        except Exception as e:
            logger.error(f"[EnhancedOrderManager] Failed to get price for {symbol}: {e}")
            return None

    def check_margin(self, symbol, quantity, price, leverage):
        try:
            account_info = self.client.account()
            balance = float(account_info['availableBalance'])
            required_margin = (quantity * price) / leverage
            if balance < required_margin:
                logger.error(f"[Margin Check] Insufficient margin for {symbol}: available={balance}, required={required_margin}")
                return False
            return True
        except Exception as e:
            logger.error(f"[Margin Check] Error for {symbol}: {e}")
            return False

    def place_enhanced_order(self, action, symbol, capital, leverage, trade_id):
        try:
            atr = get_current_atr(self.client, symbol)
            if atr is None or atr <= 0:
                logger.error(f"[EnhancedOrderManager] Failed to calculate ATR for {symbol}: {atr}")
                return None

            price = self.get_current_price(symbol)
            if not price:
                logger.error(f"[EnhancedOrderManager] Failed to get current price for {symbol}")
                return None

            # Get exchange precision rules
            rules = get_exchange_precision(self.client, symbol)
            logger.info(f"[EnhancedOrderManager] Precision rules for {symbol}: {rules}")

            # Calculate and adjust quantity
            quantity = (capital * leverage) / price
            quantity = math.floor(quantity / rules['qty_step']) * rules['qty_step']
            quantity = max(quantity, rules['min_qty'])
            quantity = round(quantity, rules['qty_precision'])

            # Special handling for specific symbols (e.g., XRPUSDT)
            if symbol == 'XRPUSDT':
                quantity = int(quantity)

            # Adjust price to tick size
            price = round(price / rules['price_tick']) * rules['price_tick']
            price = round(price, rules['price_precision'])

            # Log order details
            logger.info(f"""
[EnhancedOrderManager] Order details for {symbol}:
Original Price: {price}
Rounded Price: {round(price / rules['price_tick']) * rules['price_tick']}
Capital: {capital}
Leverage: {leverage}
Raw Quantity: {(capital * leverage) / price}
Adjusted Quantity: {quantity}
Rules: {rules}
""")

            # Check margin
            if not self.check_margin(symbol, quantity, price, leverage):
                logger.error(f"[EnhancedOrderManager] Cancellation - margin problem for {symbol}")
                return None

            side = 'BUY' if action == 'buy' else 'SELL'
            trade_id = str(trade_id).replace('trade_', '')  # Normalize trade_id
            order = self.client.new_order(
                symbol=symbol,
                side=side,
                type='MARKET',
                quantity=quantity,
                newClientOrderId=f"trade_{trade_id}",
                recvWindow=10000
            )
            order_data = {
                'order_id': str(order['orderId']),
                'symbol': symbol,
                'side': action,
                'quantity': quantity,
                'price': price,
                'timestamp': int(order.get('updateTime', order.get('transactTime', 0))),
                'status': order['status'].lower(),
                'trade_id': trade_id,
                'pnl': 0.0,
                'stop_loss': None,
                'take_profit': None,
                'is_trailing': False
            }
            logger.info(f"[EnhancedOrderManager] Order placed for {symbol}: {order_data['order_id']}")
            return order_data
        except Exception as e:
            logger.error(f"[EnhancedOrderManager] Failed to place {action} order for {symbol}: {e}")
            return None

def wait_until_order_finalized(client, symbol, order_id, max_retries=5, sleep_seconds=1):
    for _ in range(max_retries):
        order_status = client.get_order(symbol=symbol, orderId=order_id)
        status = order_status.get("status")
        if status in ["FILLED", "PARTIALLY_FILLED", "REJECTED", "CANCELED"]:
            return order_status
        time.sleep(sleep_seconds)
    return client.get_order(symbol=symbol, orderId=order_id)

def monitor_and_update_trailing_stop(client, symbol, order_id, ts_manager, trade_id=None):
    try:
        order = client.get_order(symbol=symbol, orderId=order_id)
        status = order.get("status")
        logger.info(f"[OrderManager] Order {order_id} status: {status}")
        if status == "FILLED":
            current_price = float(client.ticker_price(symbol=symbol)['price'])
            trade_id = str(trade_id or int(time.time())).replace('trade_', '')
            ts_manager.update_trailing_stop(symbol, current_price, trade_id=trade_id)
            logger.info(f"[OrderManager] Trailing stop updated for {symbol} at price {current_price}")
        return status
    except Exception as e:
        logger.error(f"[OrderManager] Error monitoring order {order_id} for {symbol}: {e}")
        return None

def check_margin(client, symbol, capital, leverage):
    try:
        account_info = client.get_account()
        available_balance = float(account_info['availableBalance'])
        required_margin = float(capital)
        logger.info(f"[Margin Check] Available Balance: {available_balance} USDT, Required Margin: {required_margin} USDT")
        send_telegram_alert(f"Margin verification for {symbol} - Available Balance: {available_balance} USDT, Required Margin: {required_margin} USDT")
        if available_balance < required_margin:
            logger.error(f"[Margin Check] Insufficient margin. Available: {available_balance} USDT, Required: {required_margin} USDT")
            send_telegram_alert(f"Margin {symbol} - Insufficient margin.")
            return False
        return True
    except Exception as e:
        logger.error(f"[Margin Check] Error: {str(e)}")
        return False

def get_min_qty(client, symbol):
    try:
        info = client.get_exchange_info()
        for s in info['symbols']:
            if s['symbol'] == symbol:
                for f in s['filters']:
                    if f['filterType'] == 'LOT_SIZE':
                        return float(f['minQty'])
    except Exception as e:
        logger.error(f"[OrderManager] Error retrieving minQty for {symbol}: {e}")
    return 0.001

client = UMFutures(key=BINANCE_API_KEY, secret=BINANCE_API_SECRET, base_url="https://testnet.binancefuture.com")

for symbol in SYMBOLS:
    try:
        ticker = client.ticker_price(symbol=symbol)
        current_price = float(ticker['price'])
        if current_price is not None and crash_protector.check_market_crash(symbol, current_price):
            ts_manager.close_position(symbol)
            send_telegram_alert(f"⚠️ CRASH DETECTED - Position closed for {symbol}")
    except Exception as e:
        logger.error(f"[OrderManager] Failed to process {symbol}: {e}")

def get_db_connection():
    try:
        conn = connection_pool.getconn()
        conn.set_client_encoding('UTF8')
        logger.debug(f"[DEBUG] Client encoding: UTF8")
        return conn
    except Exception as e:
        logger.error(f"[db_handler] Error getting database connection: {e}")
        raise

def release_db_connection(conn):
    try:
        connection_pool.putconn(conn)
    except Exception as e:
        logger.error(f"[db_handler] Error releasing database connection: {e}")

def insert_or_update_order(order):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Validation robuste de l'order_id
        order_id = str(order.get('orderId', '')) or str(order.get('order_id', ''))
        if not order_id:
            logger.error("No valid orderId found in order data")
            return

        # Validation des autres champs
        symbol = str(order.get('symbol', ''))
        status = str(order.get('status', 'UNKNOWN'))
        side = str(order.get('side', 'UNKNOWN')).upper()
        quantity = float(order.get('origQty', order.get('quantity', 0)))
        price = float(order.get('price', 0))
        timestamp = int(order.get('time', order.get('timestamp', time.time() * 1000)))
        client_order_id = str(order.get('clientOrderId', order.get('client_order_id', '')))

        query = """
        INSERT INTO orders (order_id, symbol, status, side, quantity, price, timestamp, client_order_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (order_id) DO UPDATE
        SET status = EXCLUDED.status,
            quantity = EXCLUDED.quantity,
            price = EXCLUDED.price,
            timestamp = EXCLUDED.timestamp,
            client_order_id = EXCLUDED.client_order_id;
        """
        cursor.execute(query, (order_id, symbol, status, side, quantity, price, timestamp, client_order_id))
        conn.commit()
        logger.info(f"Order {order_id} for {symbol} updated in DB.")
    except Exception as e:
        logger.error(f"[DB Error] Order update failed: {str(e)}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            release_db_connection(conn)