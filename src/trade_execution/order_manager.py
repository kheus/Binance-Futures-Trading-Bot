try:
    from src.trade_execution.order_utils import get_open_orders
    from binance.enums import SIDE_BUY, SIDE_SELL, ORDER_TYPE_MARKET
    from src.trade_execution.correlation_monitor import CorrelationMonitor
    from src.trade_execution.trade_analyzer import TradeAnalyzer
    from src.monitoring.metrics import get_current_atr, get_current_adx
    from src.trade_execution.ultra_aggressive_trailing import TrailingStopManager, format_price, format_quantity, get_exchange_precision
    from src.database.db_handler import get_db_connection, release_db_connection, wait_until_order_finalized
except ImportError:
    SIDE_BUY = 'BUY'
    SIDE_SELL = 'SELL'
    ORDER_TYPE_MARKET = 'MARKET'

import time
import logging
import yaml
import numpy as np
import pandas as pd
from pathlib import Path
from tabulate import tabulate
from binance.um_futures import UMFutures
from src.monitoring.alerting import send_telegram_alert
import psycopg2
from psycopg2 import pool
import math
import asyncio
from rich.table import Table
from rich.console import Console
import json
from src.trade_execution.market_crash_protector import MarketCrashProtector
from binance.exceptions import BinanceAPIException

console = Console()
logger = logging.getLogger(__name__)

# Load global configuration
CONFIG_PATH = Path(__file__).parent.parent.parent / "config" / "config.yaml"
with open(CONFIG_PATH, "r", encoding="utf-8-sig") as f:
    config = yaml.safe_load(f)

# Retrieve API keys and symbols
BINANCE_API_KEY = config["binance"]["api_key"]
BINANCE_API_SECRET = config["binance"]["api_secret"]
SYMBOLS = config["binance"]["symbols"]

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
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        query = """
        SELECT trade_id, symbol, side, quantity, price, stop_loss
        FROM trades
        WHERE is_trailing = TRUE AND status IN ('new', 'OPEN')
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
                adx=get_current_adx(client, symbol),
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

def place_order(signal, price, atr, client, symbol, capital, leverage, trade_id=None):
    try:
        rules = get_exchange_precision(client, symbol)
        logger.info(f"Precision rules for {symbol}: {rules}")

        price = format_price(client, symbol, price)
        qty = (capital * leverage) / price
        qty = format_quantity(client, symbol, qty)
        qty = max(qty, rules['min_qty'])

        logger.info(f"""
Order details for {symbol}:
Original Price: {price}
Rounded Price: {format_price(client, symbol, price)}
Capital: {capital}
Leverage: {leverage}
Raw Qty: {(capital * leverage) / price}
Adjusted Qty: {qty}
Rules: {rules}
""")

        # Place initial stop-loss order
        stop_loss_price = price * (1 - 0.05) if signal == 'buy' else price * (1 + 0.05)  # 5% stop-loss
        order = client.new_order(
            symbol=symbol,
            side=SIDE_SELL if signal == 'sell' else SIDE_BUY,
            type=ORDER_TYPE_MARKET,
            quantity=qty,
            recvWindow=10000
        )
        stop_order = client.new_order(
            symbol=symbol,
            side=SIDE_SELL if signal == 'buy' else SIDE_BUY,
            type='STOP_MARKET',
            quantity=qty,
            stopPrice=str(stop_loss_price),
            priceProtect=True,
            reduceOnly=True,
            newClientOrderId=f"stop_{symbol}_{int(time.time())}"
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

        avg_price = float(order_status.get('avgPrice', price))
        position_type = "long" if signal == "buy" else "short"

        ts_id = None
        stop_loss = stop_loss_price
        trade_id = trade_id or str(int(time.time()))
        has_position, position_qty = EnhancedOrderManager(client, [symbol]).check_open_position(symbol, signal, {})
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
                stop_loss = ts_manager.get_current_stop_price(symbol, trade_id=trade_id)
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
            str(order_id), symbol, signal.upper(), qty, avg_price, stop_loss, bool(ts_id), trade_id, 'OPEN' if ts_id else 'new', int(time.time() * 1000)
        ))
        conn.commit()
        release_db_connection(conn)

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
        has_position, position_qty = EnhancedOrderManager(client, [symbol]).check_open_position(symbol, signal, {})
        if not has_position:
            logger.info(f"[Trailing Stop] No open position for {symbol} on side {signal}. Skipping trailing stop update.")
            return None

        trade_id = str(trade_id or existing_sl_order_id).replace('trade_', '')
        open_orders = client.get_open_orders(symbol=symbol)
        for order in open_orders:
            if order['clientOrderId'].startswith(f"trailing_stop_{symbol}_{trade_id}"):
                logger.info(f"[Trailing Stop] Trailing stop already exists for {symbol} trade {trade_id}. Updating.")
                return ts_manager.update_trailing_stop(symbol, current_price, trade_id=trade_id)

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

def place_scaled_take_profits(client, symbol, entry_price, position_type, quantity, trade_id, levels):
    """
    Place plusieurs ordres de take profit échelonnés avec sécurité reduceOnly.
    levels: list of dicts, each with keys 'pct' (float, e.g. 0.01 for +1%), 'fraction' (float, e.g. 0.5 for 50%)
    """
    from src.trade_execution.ultra_aggressive_trailing import format_quantity, format_price, get_exchange_precision

    try:
        rules = get_exchange_precision(client, symbol)
        qty_precision = rules["qty_precision"]

        for level in levels:
            tp_price = format_price(
                client, symbol,
                entry_price * (1 + level["pct"]) if position_type == "long" else entry_price * (1 - level["pct"])
            )
            partial_qty = float(quantity) * level["fraction"]
            partial_qty = format_quantity(client, symbol, partial_qty)

            # Sécurité : éviter d’envoyer plus que la position actuelle
            positions = client.get_position_risk(symbol=symbol)
            pos_qty = 0.0
            for pos in positions:
                if pos["symbol"] == symbol:
                    pos_qty = abs(float(pos["positionAmt"]))
                    break

            if partial_qty > pos_qty:
                partial_qty = pos_qty

            if partial_qty <= 0:
                continue

            order = client.new_order(
                symbol=symbol,
                side="SELL" if position_type == "long" else "BUY",
                type="TAKE_PROFIT_MARKET",
                stopPrice=str(tp_price),
                quantity=str(partial_qty),
                priceProtect=True,
                reduceOnly=True,   # ✅ Sécurisation ici
                newClientOrderId=f"tp_{symbol}_{trade_id}_{int(time.time())}"
            )

            logger.info(f"[{symbol}] ✅ Scaled TP placed at {tp_price} for {partial_qty}, trade_id: {trade_id}")

    except Exception as e:
        logger.error(f"[{symbol}] ❌ Failed to place scaled take-profits: {e}")

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

def log_order_details(symbol, price, capital, leverage, quantity, rules):
    rounded_price = format_price(client, symbol, price)
    raw_qty = (capital * leverage) / price

    table = Table(title=f"🧾 Order Details for {symbol}", show_lines=True)
    table.add_column("Field", style="cyan", no_wrap=True)
    table.add_column("Value", style="magenta")

    table.add_row("Original Price", f"{price:.4f}")
    table.add_row("Rounded Price", f"{rounded_price:.4f}")
    table.add_row("Capital", f"{capital}")
    table.add_row("Leverage", f"{leverage}")
    table.add_row("Raw Quantity", f"{raw_qty:.4f}")
    table.add_row("Adjusted Quantity", f"{quantity}")
    table.add_row("Rules", json.dumps(rules, indent=2))

    console.log(table)

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
        self.current_positions = {symbol: None for symbol in symbols}

    def get_current_price(self, symbol):
        try:
            ticker = self.client.ticker_price(symbol=symbol)
            price = float(ticker['price'])
            logger.debug(f"[OrderManager] Fetched price for {symbol}: {price}")
            return price
        except BinanceAPIException as e:
            logger.error(f"[EnhancedOrderManager] Failed to get price for {symbol}: {e}")
            return None

    def check_margin(self, symbol, quantity, price, leverage):
        try:
            account_info = self.client.account()
            balance = float(next(a['availableBalance'] for a in account_info['assets'] if a['asset'] == 'USDT'))
            required_margin = (quantity * price) / leverage
            if balance < required_margin:
                logger.error(f"[Margin Check] Insufficient margin for {symbol}: available={balance}, required={required_margin}")
                return False
            return True
        except BinanceAPIException as e:
            logger.error(f"[Margin Check] Error for {symbol}: {e}")
            return False

    def place_enhanced_order(self, action, symbol, capital, leverage, trade_id):
        try:
            atr = get_current_atr(self.client, symbol)
            if atr is None or atr <= 0:
                logger.error(f"[EnhancedOrderManager] Failed to calculate ATR for {symbol}: {atr}")
                return None

            adx = get_current_adx(self.client, symbol)
            if adx is None or adx <= 0:
                logger.error(f"[EnhancedOrderManager] Failed to calculate ADX for {symbol}: {adx}")
                return None

            price = self.get_current_price(symbol)
            if not price:
                logger.error(f"[EnhancedOrderManager] Failed to get current price for {symbol}")
                return None

            rules = get_exchange_precision(self.client, symbol)
            logger.info(f"[EnhancedOrderManager] Precision rules for {symbol}: {rules}")

            quantity = format_quantity(self.client, symbol, (capital * leverage) / price)
            quantity = max(quantity, rules['min_qty'])
            price = format_price(client, symbol, price)

            if not self.check_margin(symbol, quantity, price, leverage):
                logger.error(f"[EnhancedOrderManager] Cancellation - margin problem for {symbol}")
                return None

            side = 'BUY' if action == 'buy' else 'SELL'
            trade_id = str(trade_id).replace('trade_', '')
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
        except BinanceAPIException as e:
            logger.error(f"[EnhancedOrderManager] Failed to place {action} order for {symbol}: {e}")
            return None

    def check_open_position(self, symbol, side, current_positions):
        position_qty = 0.0
        has_position = False
        try:
            position_info = self.client.get_position_risk(symbol=symbol)
            if position_info is None:
                logger.warning(f"[Position Check] No position data returned for {symbol}")
                return False, 0.0
            for pos in position_info:
                if pos['symbol'] == symbol:
                    qty = float(pos['positionAmt'])
                    if qty != 0:
                        api_position_side = 'long' if qty > 0 else 'short'
                        has_position = True
                        position_qty = abs(qty)
                        current_positions[symbol] = {
                            'side': api_position_side,
                            'quantity': position_qty,
                            'price': float(pos['entryPrice']),
                            'trade_id': current_positions[symbol]['trade_id'] if current_positions.get(symbol) and 'trade_id' in current_positions[symbol] else None
                        }
                        logger.debug(f"[Position Check] Updated current_positions for {symbol}: {current_positions[symbol]}")
                        break
            if not has_position and current_positions.get(symbol):
                logger.info(f"[Position Check] No active position for {symbol}, clearing current_positions")
                current_positions[symbol] = None
            return has_position, position_qty
        except BinanceAPIException as e:
            logger.error(f"[Position Check Error] For {symbol}: {e}")
            return False, 0.0
        except Exception as e:
            logger.error(f"[Position Check Error] For {symbol}: {str(e)}")
            return False, 0.0

    def clean_orphaned_trailing_stops(self, ts_manager):
        for symbol in self.symbols:
            has_position, _ = self.check_open_position(symbol, None, self.current_positions)
            if not has_position and ts_manager.has_trailing_stop(symbol):
                logger.warning(f"[OrderManager] No position found for {symbol}, removing trailing stop")
                ts_manager.close_position(symbol)
                self.current_positions[symbol] = None

def wait_until_order_finalized(client, symbol, order_id, max_retries=5, sleep_seconds=1):
    for _ in range(max_retries):
        try:
            order_status = client.get_order(symbol=symbol, orderId=order_id)
            status = order_status.get("status")
            if status in ["FILLED", "PARTIALLY_FILLED", "REJECTED", "CANCELED"]:
                return order_status
            time.sleep(sleep_seconds)
        except BinanceAPIException as e:
            logger.error(f"[OrderManager] Failed to check order status for {order_id}: {e}")
            time.sleep(sleep_seconds)
    try:
        return client.get_order(symbol=symbol, orderId=order_id)
    except BinanceAPIException as e:
        logger.error(f"[OrderManager] Final attempt to check order status failed for {order_id}: {e}")
        return None

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
    except BinanceAPIException as e:
        logger.error(f"[OrderManager] Error monitoring order {order_id} for {symbol}: {e}")
        return None

def check_margin(client, symbol, capital, leverage):
    try:
        account_info = client.account()
        balance = float(next(a['availableBalance'] for a in account_info['assets'] if a['asset'] == 'USDT'))
        required_margin = float(capital)
        logger.info(f"[Margin Check] Available Balance: {balance} USDT, Required Margin: {required_margin} USDT")
        send_telegram_alert(f"Margin verification for {symbol} - Available Balance: {balance} USDT, Required Margin: {required_margin} USDT")
        if balance < required_margin:
            logger.error(f"[Margin Check] Insufficient margin. Available: {balance} USDT, Required: {required_margin} USDT")
            send_telegram_alert(f"Margin {symbol} - Insufficient margin.")
            return False
        return True
    except Exception as e:
        logger.error(f"[Margin Check] Error: {str(e)}")
        return False

def get_min_qty(client, symbol):
    try:
        info = client.exchange_info()
        for s in info['symbols']:
            if s['symbol'] == symbol:
                for f in s['filters']:
                    if f['filterType'] == 'LOT_SIZE':
                        return float(f['minQty'])
    except BinanceAPIException as e:
        logger.error(f"[OrderManager] Error retrieving minQty for {symbol}: {e}")
    return 0.001

client = UMFutures(key=BINANCE_API_KEY, secret=BINANCE_API_SECRET, base_url="https://fapi.binance.com")

for symbol in SYMBOLS:
    try:
        ticker = client.ticker_price(symbol=symbol)
        current_price = float(ticker['price'])
        if current_price is not None and crash_protector.check_market_crash(symbol, current_price):
            ts_manager.close_position(symbol)
            send_telegram_alert(f"⚠️ CRASH DETECTED - Position closed for {symbol}")
    except BinanceAPIException as e:
        logger.error(f"[OrderManager] Failed to process {symbol}: {e}")

def insert_or_update_order(order):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        order_id = str(order.get('orderId', '')) or str(order.get('order_id', ''))
        if not order_id:
            logger.error("No valid orderId found in order data")
            return
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