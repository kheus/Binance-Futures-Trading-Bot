import logging
import time
from binance.um_futures import UMFutures

logger = logging.getLogger(__name__)

class UltraAgressiveTrailingStop:
    def __init__(self, client, symbol, initial_percentage=2.0, max_retries=3):
        self.client = client
        self.symbol = symbol
        self.percentage = initial_percentage / 100
        self.max_retries = max_retries
        self.trailing_stop_order_id = None
        self.entry_price = 0.0
        self.position_type = None
        self.quantity = 0.0

    def initialize_trailing_stop(self, entry_price, position_type, quantity, atr):
        self.entry_price = float(entry_price)
        self.position_type = position_type
        self.quantity = format_quantity(self.symbol, float(quantity))

        if self.entry_price <= 0:
            logger.error(f"[{self.symbol}] ❌ Invalid entry_price: {self.entry_price}")
            return None

        if atr <= 0:
            logger.warning(f"[{self.symbol}] ⚠️ ATR invalid ({atr}), using fallback calculation.")
            atr = self.entry_price * 0.02  # fallback ATR = 2%

        try:
            current_price = float(self.client.ticker_price(symbol=self.symbol)['price'])
        except Exception as e:
            logger.error(f"[{self.symbol}] ❌ Failed to fetch current price: {e}")
            return None

        if current_price <= 0:
            logger.error(f"[{self.symbol}] ❌ Invalid current_price: {current_price}")
            return None

        price_adjustment = max(atr * (self.percentage * 10), 0.1 * current_price)
        stop_price = format_price(self.symbol, self.entry_price - price_adjustment if position_type == 'long' else self.entry_price + price_adjustment)

        if stop_price <= 0:
            logger.error(f"[{self.symbol}] ❌ stop_price={stop_price} invalid.")
            return None

        notional_value = stop_price * self.quantity
        reduce_only = notional_value < 100

        # Vérification du solde disponible
        try:
            account = self.client.balance()
            usdt_balance = next((float(x['balance']) for x in account if x['asset'] == 'USDT'), 0)
        except Exception as e:
            logger.warning(f"[{self.symbol}] ⚠️ Could not fetch balance: {e}")
            usdt_balance = 0

        if notional_value > usdt_balance * 50:  # en cross 50x
            logger.warning(f"[{self.symbol}] ❌ Not enough margin (need ≈ {notional_value}, have ≈ {usdt_balance}). Skipping trailing stop.")
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
                    reduceOnly=reduce_only
                )
                self.trailing_stop_order_id = order['orderId']
                logger.info(f"[{self.symbol}] ✅ Trailing stop placed (order {self.trailing_stop_order_id}) at {stop_price} (Qty: {self.quantity})")
                return self.trailing_stop_order_id
            except Exception as e:
                msg = str(e)
                if "Margin is insufficient" in msg:
                    logger.error(f"[{self.symbol}] ❌ Margin insuffisante. Annulation du trailing stop.")
                    return None
                logger.error(f"[{self.symbol}] Retry {attempt+1}/{self.max_retries} failed: {e}")
                time.sleep(1 + attempt)
        return None

    def update_trailing_stop(self, current_price):
        if self.trailing_stop_order_id and self.entry_price:
            try:
                current_price = float(current_price)
                if current_price <= 0:
                    logger.error(f"[{self.symbol}] ❌ Invalid current_price: {current_price}")
                    return

                if self.position_type == 'long':
                    new_stop = format_price(self.symbol, current_price * (1 - self.percentage))
                    if new_stop > self.entry_price:
                        self.client.cancel_order(symbol=self.symbol, orderId=self.trailing_stop_order_id)
                        order = self.client.new_order(
                            symbol=self.symbol,
                            side='SELL',
                            type='STOP_MARKET',
                            quantity=str(self.quantity),
                            stopPrice=str(new_stop),
                            priceProtect=True,
                            reduceOnly=True
                        )
                        self.trailing_stop_order_id = order['orderId']
                        self.entry_price = new_stop
                        logger.info(f"[{self.symbol}] 🔄 Trailing stop updated (long) to {new_stop}")
                elif self.position_type == 'short':
                    new_stop = format_price(self.symbol, current_price * (1 + self.percentage))
                    if new_stop < self.entry_price:
                        self.client.cancel_order(symbol=self.symbol, orderId=self.trailing_stop_order_id)
                        order = self.client.new_order(
                            symbol=self.symbol,
                            side='BUY',
                            type='STOP_MARKET',
                            quantity=str(self.quantity),
                            stopPrice=str(new_stop),
                            priceProtect=True,
                            reduceOnly=True
                        )
                        self.trailing_stop_order_id = order['orderId']
                        self.entry_price = new_stop
                        logger.info(f"[{self.symbol}] 🔄 Trailing stop updated (short) to {new_stop}")
            except Exception as e:
                logger.error(f"[{self.symbol}] ❌ Failed to update trailing stop: {e}")

class TrailingStopManager:
    def __init__(self, client):
        self.client = client
        self.stops = {}

    def has_trailing_stop(self, symbol):
        return symbol in self.stops and self.stops[symbol].trailing_stop_order_id is not None

    def initialize_trailing_stop(self, symbol, entry_price, position_type, quantity, atr):
        if symbol not in self.stops:
            self.stops[symbol] = UltraAgressiveTrailingStop(self.client, symbol)
        return self.stops[symbol].initialize_trailing_stop(entry_price, position_type, quantity, atr)

    def update_trailing_stop(self, symbol, current_price):
        if symbol in self.stops:
            self.stops[symbol].update_trailing_stop(current_price)

    def close_position(self, symbol):
        if symbol in self.stops:
            try:
                self.client.cancel_order(symbol=symbol, orderId=self.stops[symbol].trailing_stop_order_id)
            except Exception as e:
                logger.error(f"[{symbol}] ❌ Failed to cancel trailing stop: {e}")
            del self.stops[symbol]
            logger.info(f"[{symbol}] ✅ Trailing stop closed.")

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
