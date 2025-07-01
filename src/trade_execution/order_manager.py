try:
    # Dans order_manager.py
    from src.trade_execution.order_utils import get_open_orders   
    from binance.enums import SIDE_BUY, SIDE_SELL, ORDER_TYPE_MARKET
    from src.trade_execution.correlation_monitor import CorrelationMonitor
    from src.trade_execution.trade_analyzer import TradeAnalyzer
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

def init_trailing_stop_manager(client):
    global ts_manager
    ts_manager = TrailingStopManager(client)  # Utiliser TrailingStopManager au lieu de UltraAgressiveTrailingStop
    return ts_manager

def get_tick_info(client, symbol):
    try:
        exchange_info = client.exchange_info()
        symbol_info = next((s for s in exchange_info["symbols"] if s["symbol"] == symbol), None)
        if not symbol_info:
            raise ValueError(f"Symbol {symbol} not found in exchange info.")
        price_filter = next(f for f in symbol_info['filters'] if f['filterType'] == 'PRICE_FILTER')
        return float(price_filter['tickSize']), int(symbol_info['quantityPrecision'])
    except Exception as e:
        logger.error(f"[OrderManager] Error getting tick info for {symbol}: {e}")
        return 0.0001, 8  # Fallback values

def round_to_tick(value, tick_size):
    return round(round(value / tick_size) * tick_size, 8)

def place_order(signal, price, atr, client, symbol, capital, leverage):
    try:
        # 1. Enhanced margin check
        if not check_margin(client, symbol, capital, leverage):
            logger.error("Cancellation - margin problem")
            return None

        # 2. Secure quantity calculation
        tick_size, qty_precision = get_tick_info(client, symbol)
        base_qty = capital / price
        qty = round(base_qty * leverage, qty_precision)

        # 3. Minimum check (uses exchange info for robustness)
        exchange_info = client.exchange_info()
        symbol_info = next((s for s in exchange_info["symbols"] if s["symbol"] == symbol), None)
        min_qty = 1.0
        if symbol_info:
            lot_size = next((f for f in symbol_info["filters"] if f["filterType"] == "LOT_SIZE"), None)
            if lot_size:
                min_qty = float(lot_size["minQty"])
        if qty < min_qty:
            logger.warning(f"Quantité ajustée à {min_qty} (min)")
            qty = min_qty

        # 4. Order placement with timeout
        order = client.new_order(
            symbol=symbol,
            side=SIDE_BUY if signal == "buy" else SIDE_SELL,
            type=ORDER_TYPE_MARKET,
            quantity=qty,
            recvWindow=5000  # Increased timeout
        )

        # Alternative response handling
        order_id = order.get('orderId') or order.get('clientOrderId')
        if not order_id:
            raise ValueError("Invalid order response from Binance")

        # Wait for the order to be finalized
        order_status = wait_until_order_finalized(client, symbol, order_id)

        if isinstance(order_status, dict) and order_status.get("status") == "REJECTED":
            logger.error(f"[OrderManager] Order rejected: {order_status.get('msg', 'Unknown reason')}")
            return None

        if isinstance(order_status, dict) and order_status.get("status") == "PARTIALLY_FILLED":
            logger.warning(f"[OrderManager] Partial fill: {order_status['executedQty']}/{order_status['origQty']}")
            qty = float(order_status["executedQty"])

        avg_price = get_average_fill_price(client, symbol, order_id) or price
        position_type = "long" if signal == "buy" else "short"

        ts_id = ts_manager.initialize_trailing_stop(
            symbol=symbol,
            entry_price=avg_price,
            position_type=position_type,
            quantity=qty,
            atr=atr
        )
        if not ts_id:
            logger.error(f"[OrderManager] Trailing stop init failed. Canceling order.")
            try:
                client.cancel_order(symbol=symbol, orderId=order_id)
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
            "is_trailing": True,
            "stop_loss": None,
            "take_profit": None,
            "pnl": 0.0
        }
        logger.info(f"[OrderManager] Successfully placed {signal} order for {symbol}: {order_details}")
        logger.warning(f"TELEGRAM ALERT: Order placed for {symbol} - {signal.upper()} at {avg_price:.2f} USDT, Qty: {qty:.4f} with ATR: {atr:.2f}")
        log_order_as_table(signal, symbol, price, atr, qty, order_details)
        track_order_locally(order_details)
        return order_details

    except Exception as e:
        logger.error(f"Échec placement ordre: {str(e)}")
        send_telegram_alert(f"Order placement failed for {symbol}: {str(e)}")
        logger.debug(f"Full Binance response: {locals().get('order', {})}")
        import traceback
        logger.error(traceback.format_exc())
        return None

def update_trailing_stop(client, symbol, signal, current_price, atr, base_qty, existing_sl_order_id):
    global ts_manager

    if ts_manager is None:
        ts_manager = init_trailing_stop_manager(client)

    try:
        if existing_sl_order_id in (-1, None):
            position_type = "long" if signal == "buy" else "short"
            return ts_manager.initialize_trailing_stop(
                symbol=symbol,
                entry_price=current_price,
                position_type=position_type,
                quantity=base_qty,
                atr=atr
            )
        return ts_manager.update_trailing_stop(symbol, current_price)
    except Exception as e:
        logger.error(f"[OrderManager] Error updating trailing stop for {symbol}: {e}")
        return None

def place_scaled_take_profits(client, symbol, entry_price, quantity, position_type, atr, ts_manager):
    try:
        tp_levels = [
            {"factor": 1.5, "percent": 0.4, "reduce_ts": True},
            {"factor": 3.0, "percent": 0.4, "reduce_ts": True},
            {"factor": 5.0, "percent": 0.2, "close_position": True}
        ]
        for level in tp_levels:
            tp_price = entry_price + (level["factor"] * atr) if position_type == "long" else entry_price - (level["factor"] * atr)
            partial_qty = quantity * level["percent"]
            client.new_order(
                symbol=symbol,
                side=SIDE_SELL if position_type == "long" else SIDE_BUY,
                type="TAKE_PROFIT_MARKET",
                stopPrice=str(tp_price),
                quantity=partial_qty,
                closePosition=level.get("close_position", False),
                priceProtect=True
            )
            if level.get("reduce_ts"):
                ts_manager.adjust_quantity(symbol, quantity * (1 - level["percent"]))
        logger.info(f"[OrderManager] Placed scaled take-profits for {symbol}")
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
            "timestamp": int(time.time() * 1000)
        }
        logger.info(f"[OrderManager] Locally tracked order: {order_tracking[order_id]}")
    except KeyError as e:
        logger.error(f"Invalid order format - missing key {e}")

class EnhancedOrderManager:
    def __init__(self, client, symbols):
        self.client = client
        self.ts_manager = TrailingStopManager(client)  # Utiliser TrailingStopManager
        self.crash_protector = MarketCrashProtector()
        self.correlation_monitor = CorrelationMonitor(symbols)
        self.trade_analyzer = TradeAnalyzer()

    def get_current_price(self, symbol):
        try:
            ticker = self.client.ticker_price(symbol=symbol)
            price = float(ticker['price'])
            return price
        except Exception as e:
            logger.error(f"[EnhancedOrderManager] Failed to get current price for {symbol}: {e}")
            return None

    def is_overexposed(self, symbol, corr_matrix):
        try:
            if corr_matrix is None or symbol not in corr_matrix.index:
                return False
            high_corr = corr_matrix[symbol][corr_matrix[symbol] > 0.8].index.tolist()
            if len(high_corr) > 2:  # More than 2 highly correlated symbols
                logger.warning(f"[EnhancedOrderManager] Overexposure risk for {symbol} with {high_corr}")
                return True
            return False
        except Exception as e:
            logger.error(f"[EnhancedOrderManager] Error checking exposure for {symbol}: {e}")
            return False

    def dynamic_position_sizing(self, symbol, capital):
        try:
            price = self.get_current_price(symbol)
            if price is None or price == 0:
                logger.error(f"[EnhancedOrderManager] Invalid price for {symbol}")
                return 0
            return capital / price
        except Exception as e:
            logger.error(f"[EnhancedOrderManager] Error calculating position size for {symbol}: {e}")
            return 0

    def place_enhanced_order(self, signal, symbol, capital, leverage):
        try:
            corr_matrix = self.correlation_monitor.get_correlation_matrix()
            if self.is_overexposed(symbol, corr_matrix):
                logger.warning(f"[EnhancedOrderManager] Skipping order for {symbol} due to overexposure")
                return None
            atr = self.get_current_atr(symbol)
            price = self.get_current_price(symbol)
            if price is None or atr is None:
                logger.error(f"[EnhancedOrderManager] Invalid price or ATR for {symbol}")
                return None
            quantity = self.dynamic_position_sizing(symbol, capital)
            order = place_order(signal, price, atr, self.client, symbol, quantity, leverage)
            if order:
                self.ts_manager.initialize_trailing_stop(
                    symbol=symbol,
                    entry_price=order["price"],
                    position_type="long" if signal == "buy" else "short",
                    quantity=order["quantity"],
                    atr=atr
                )
                self.crash_protector.check_market_crash(symbol, price)
                place_scaled_take_profits(self.client, symbol, order["price"], order["quantity"], signal, atr, self.ts_manager)
                self.trade_analyzer.add_trade(order)
                logger.info(f"[EnhancedOrderManager] Placed enhanced order for {symbol}: {order}")
            return order
        except Exception as e:
            logger.error(f"[EnhancedOrderManager] Error placing enhanced order for {symbol}: {e}")
            return None

    def get_current_atr(self, symbol):
        try:
            klines = self.client.klines(symbol=symbol, interval='1h', limit=14)
            if len(klines) < 14:
                logger.warning(f"Insufficient data for ATR calculation for {symbol}: {len(klines)}")
                return 0.0
            df = pd.DataFrame(klines, columns=["open_time", "open", "high", "low", "close", "volume",
                                              "close_time", "quote_asset_vol", "num_trades", "taker_buy_base_vol",
                                              "taker_buy_quote_vol", "ignore"])
            df = df[["high", "low", "close"]].astype(float)
            atr = talib.ATR(df["high"], df["low"], df["close"], timeperiod=14)[-1]
            return float(atr) if not pd.isna(atr) else 0.0
        except Exception as e:
            logger.error(f"[EnhancedOrderManager] Failed to calculate ATR for {symbol}: {e}")
            return 0.0

def wait_until_order_finalized(client, symbol, order_id, max_retries=5, sleep_seconds=1):
    """
    Attend que l'ordre soit dans un état final (FILLED, PARTIALLY_FILLED, REJECTED, CANCELED).
    Retourne le status final de l'ordre.
    """
    for _ in range(max_retries):
        order_status = client.query_order(symbol=symbol, orderId=order_id)
        status = order_status.get("status")
        if status in ["FILLED", "PARTIALLY_FILLED", "REJECTED", "CANCELED"]:
            return order_status
        time.sleep(sleep_seconds)
    # Dernière vérification après les retries
    return client.get_order(symbol=symbol, orderId=order_id)

def monitor_and_update_trailing_stop(client, symbol, order_id, ts_manager):
    """
    Surveille l'ordre jusqu'à exécution et applique le trailing stop.
    """
    try:
        order = client.query_order(symbol=symbol, orderId=order_id)
        status = order.get("status")
        logger.info(f"[OrderManager] Order {order_id} status: {status}")
        if status == "FILLED":
            current_price = float(client.ticker_price(symbol=symbol)['price'])
            # Exemple : appliquer le trailing stop
            ts_manager.update_trailing_stop(symbol, current_price)
            logger.info(f"[OrderManager] Trailing stop updated for {symbol} at price {current_price}")
        return status
    except Exception as e:
        logger.error(f"[OrderManager] Error monitoring order {order_id} for {symbol}: {e}")
        return None

def check_margin(client, symbol, capital, leverage):
    """
    Verify that the available margin on the Futures account is sufficient to place the order.
    """
    try:
        # 1. Retrieve the available balance on the Futures account
        account_info = client.account()
        available_balance = float(account_info['availableBalance'])

        # 2. Calculate the required margin for the order (capital used for the position)
        required_margin = float(capital)  # For Futures, the required margin = capital used

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
    """
    Return the minimum quantity allowed for a given symbol according to Binance Futures rules.
    """
    try:
        info = client.exchange_info()
        for s in info['symbols']:
            if s['symbol'] == symbol:
                for f in s['filters']:
                    if f['filterType'] == 'LOT_SIZE':
                        return float(f['minQty'])
    except Exception as e:
        logger.error(f"[OrderManager] Error retrieving minQty for {symbol}: {e}")
    return 0.001  # Valeur par défaut sécurisée

# Ensure you have a Binance client instance before this loop.
# For example, if you use python-binance:
client = UMFutures(key=BINANCE_API_KEY, secret=BINANCE_API_SECRET)

for symbol in SYMBOLS:
    try:
        ticker = client.ticker_price(symbol=symbol)
        current_price = float(ticker['price'])
        if current_price is not None and crash_protector.check_market_crash(symbol, current_price):
            ts_manager.close_position(symbol)
            def send_telegram_alert(message):
                logger.warning(f"TELEGRAM ALERT: {message}")
            send_telegram_alert(f"⚠️ CRASH DETECTED - Position closed for {symbol}")
    except Exception as e:
        logger.error(f"[OrderManager] Failed to process {symbol}: {e}")