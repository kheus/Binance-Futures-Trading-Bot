# File: src/trade_execution/ultra_aggressive_trailing.py
import logging
import time
from binance.um_futures import UMFutures
from src.database.db_handler import update_trade_on_close

logger = logging.getLogger(__name__)

class UltraAgressiveTrailingStop:
    def __init__(self, client, symbol, trailing_distance=0.01):
        self.client = client
        self.symbol = symbol
        self.trailing_distance = trailing_distance
        self.max_retries = 3
        self.trailing_stop_order_id = None
        self.entry_price = 0.0
        self.current_stop_price = 0.0
        self.position_type = None
        self.quantity = 0.0
        self.trade_id = None

    def initialize_trailing_stop(self, entry_price, position_type, quantity, atr, trade_id):
        self.entry_price = float(entry_price)
        self.position_type = position_type
        self.quantity = format_quantity(self.symbol, float(quantity))
        self.trade_id = trade_id

        if self.entry_price <= 0:
            logger.error(f"[{self.symbol}] ❌ Invalid entry_price: {self.entry_price}")
            return None

        try:
            current_price = float(self.client.ticker_price(symbol=self.symbol)['price'])
        except Exception as e:
            logger.error(f"[{self.symbol}] ❌ Failed to fetch current price: {e}")
            return None

        if current_price <= 0:
            logger.error(f"[{self.symbol}] ❌ Invalid current_price: {current_price}")
            return None

        stop_price = format_price(self.symbol, 
            self.entry_price * (1 - self.trailing_distance) if position_type == 'long' 
            else self.entry_price * (1 + self.trailing_distance))

        if stop_price <= 0:
            logger.error(f"[{self.symbol}] ❌ stop_price={stop_price} invalid.")
            return None

        try:
            account = self.client.balance()
            usdt_balance = next((float(x['balance']) for x in account if x['asset'] == 'USDT'), 0)
        except Exception as e:
            logger.warning(f"[{self.symbol}] ⚠️ Could not fetch balance: {e}")
            usdt_balance = 0

        notional_value = stop_price * self.quantity
        if notional_value > usdt_balance * 50:
            logger.warning(f"[{self.symbol}] ❌ Not enough margin (need ≈ {notional_value}, have ≈ {usdt_balance}).")
            return None

        for attempt in range(self.max_retries):
            try:
                order = self.client.new_order(
                    symbol=self.symbol,
                    side='SELL' if position_type == 'long' else 'BUY',
                    type='STOP_MARKET',
                    quantity=str(self.quantity),
                    stopPrice=str(stop_price),
                    priceProtect=True,
                    reduceOnly=True
                )
                self.trailing_stop_order_id = order['orderId']
                self.current_stop_price = stop_price
                logger.info(f"[{self.symbol}] ✅ Trailing stop placed (order {self.trailing_stop_order_id}) at {stop_price} (Qty: {self.quantity}, trade_id: {self.trade_id})")
                return self.trailing_stop_order_id
            except Exception as e:
                msg = str(e)
                if "Margin is insufficient" in msg:
                    logger.error(f"[{self.symbol}] ❌ Margin insuffisante. Annulation.")
                    return None
                logger.error(f"[{self.symbol}] Retry {attempt+1}/{self.max_retries} failed: {e}")
                time.sleep(1 + attempt)
        return None

    def update_trailing_stop(self, current_price, trade_id=None):
        if not self.trailing_stop_order_id or not self.entry_price:
            logger.error(f"[{self.symbol}] ❌ No trailing stop active or invalid entry_price")
            return

        if trade_id and trade_id != self.trade_id:
            logger.warning(f"[{self.symbol}] ⚠️ Trade ID mismatch: expected {self.trade_id}, got {trade_id}")
            return

        try:
            current_price = float(current_price)
            if current_price <= 0:
                logger.error(f"[{self.symbol}] ❌ Invalid current_price: {current_price}")
                return

            new_stop = format_price(
                self.symbol,
                current_price * (1 - self.trailing_distance) if self.position_type == 'long'
                else current_price * (1 + self.trailing_distance)
            )

            should_update = (
                (self.position_type == 'long' and new_stop > self.current_stop_price) or
                (self.position_type == 'short' and new_stop < self.current_stop_price)
            )

            if should_update:
                for attempt in range(self.max_retries):
                    try:
                        self.client.cancel_order(symbol=self.symbol, orderId=self.trailing_stop_order_id)
                        logger.info(f"[{self.symbol}] ❌ Old trailing stop order {self.trailing_stop_order_id} canceled.")

                        order = self.client.new_order(
                            symbol=self.symbol,
                            side='SELL' if self.position_type == 'long' else 'BUY',
                            type='STOP_MARKET',
                            quantity=str(self.quantity),
                            stopPrice=str(new_stop),
                            priceProtect=True,
                            reduceOnly=True
                        )
                        self.trailing_stop_order_id = order['orderId']
                        self.current_stop_price = new_stop
                        logger.info(f"[{self.symbol}] 🔄 Trailing stop updated to {new_stop} (order {self.trailing_stop_order_id}, trade_id: {self.trade_id})")
                        return
                    except Exception as e:
                        logger.error(f"[{self.symbol}] Retry {attempt + 1}/{self.max_retries} failed to update trailing stop: {e}")
                        time.sleep(1 + attempt)
            else:
                logger.debug(f"[{self.symbol}] No update needed: new_stop={new_stop} not better than current_stop_price={self.current_stop_price}")
        except Exception as e:
            logger.error(f"[{self.symbol}] ❌ Failed to update trailing stop: {e}")

    def verify_order_execution(self):
        try:
            if not self.trailing_stop_order_id:
                logger.error(f"[{self.symbol}] ❌ No trailing stop order to verify")
                return False
            order = self.client.query_order(symbol=self.symbol, orderId=self.trailing_stop_order_id)
            status = order.get('status')
            if status == 'FILLED':
                exit_price = float(order.get('avgPrice', self.current_stop_price))
                success = update_trade_on_close(
                    symbol=self.symbol,
                    order_id=self.trade_id,
                    exit_price=exit_price,
                    quantity=self.quantity,
                    side='BUY' if self.position_type == 'long' else 'SELL',
                    leverage=50
                )
                if success:
                    logger.info(f"[{self.symbol}] ✅ Trailing stop order {self.trailing_stop_order_id} executed at {exit_price}, trade_id: {self.trade_id}")
                    self.trailing_stop_order_id = None
                    self.current_stop_price = 0.0
                    return True
                else:
                    logger.error(f"[{self.symbol}] ❌ Failed to update trade for trade_id {self.trade_id}")
                    return False
            elif status in ['CANCELED', 'REJECTED', 'EXPIRED']:
                logger.warning(f"[{self.symbol}] ⚠️ Trailing stop order {self.trailing_stop_order_id} {status.lower()}")
                self.trailing_stop_order_id = None
                self.current_stop_price = 0.0
                return False
            return False
        except Exception as e:
            logger.error(f"[{self.symbol}] ❌ Failed to verify order execution: {e}")
            return False

class TrailingStopManager:
    def __init__(self, client):
        self.client = client
        self.stops = {}

    def has_trailing_stop(self, symbol):
        return symbol in self.stops and self.stops[symbol].trailing_stop_order_id is not None

    def initialize_trailing_stop(self, symbol, entry_price, position_type, quantity, atr, trade_id):
        if symbol not in self.stops:
            self.stops[symbol] = UltraAgressiveTrailingStop(self.client, symbol)
        return self.stops[symbol].initialize_trailing_stop(entry_price, position_type, quantity, atr, trade_id)

    def update_trailing_stop(self, symbol, current_price, trade_id=None):
        if symbol in self.stops:
            self.stops[symbol].update_trailing_stop(current_price, trade_id=trade_id)
            if self.stops[symbol].verify_order_execution():
                self.close_position(symbol)

    def close_position(self, symbol):
        if symbol in self.stops:
            try:
                if self.stops[symbol].trailing_stop_order_id:
                    self.client.cancel_order(symbol=symbol, orderId=self.stops[symbol].trailing_stop_order_id)
                    logger.info(f"[{symbol}] ✅ Trailing stop order canceled")
            except Exception as e:
                logger.error(f"[{symbol}] ❌ Failed to cancel trailing stop: {e}")
            del self.stops[symbol]
            logger.info(f"[{symbol}] ✅ Position closed")

def init_trailing_stop_manager(client):
    return TrailingStopManager(client)

def format_price(symbol, price):
    if symbol in ['BTCUSDT', 'ETHUSDT']:
        return round(float(price), 2)
    return round(float(price), 4)

def format_quantity(symbol, quantity):
    if symbol in ['BTCUSDT', 'ETHUSDT']:
        return round(float(quantity), 3)
    return round(float(quantity), 2)