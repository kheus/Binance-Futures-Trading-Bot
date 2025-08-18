import logging
import time
import random
from binance.um_futures import UMFutures
from binance.exceptions import BinanceAPIException
from src.database.db_handler import update_trade_on_close, connection_pool
from src.monitoring.metrics import get_current_atr, get_current_adx
from src.monitoring.alerting import send_telegram_alert

logger = logging.getLogger(__name__)

# === UTILITAIRES GLOBAUX ===
def get_exchange_precision(client, symbol):
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
        return {
            'price_tick': 0.0001,
            'qty_step': 1.0,
            'min_qty': 1.0,
            'price_precision': 4,
            'qty_precision': 0
        }

def format_price(client, symbol, price):
    try:
        rules = get_exchange_precision(client, symbol)
        return round(float(price), rules['price_precision'])
    except Exception as e:
        logger.warning(f"[{symbol}] ⚠️ Could not fetch price precision: {e}. Using default precision.")
        if symbol in ['BTCUSDT', 'ETHUSDT']:
            return round(float(price), 2)
        return round(float(price), 4)

def format_quantity(client, symbol, quantity):
    try:
        rules = get_exchange_precision(client, symbol)
        qty = round(float(quantity), rules['qty_precision'])
        if symbol in ['BTCUSDT', 'ETHUSDT']:
            qty = round(float(quantity), 3)
        return qty
    except Exception as e:
        logger.warning(f"[{symbol}] ⚠️ Could not fetch quantity precision: {e}. Using default precision.")
        if symbol in ['BTCUSDT', 'ETHUSDT']:
            return round(float(quantity), 3)
        return round(float(quantity), 2)

def get_spread(client, symbol):
    try:
        book = client.get_order_book(symbol=symbol, limit=5)
        bid = float(book['bids'][0][0])
        ask = float(book['asks'][0][0])
        return ask - bid
    except Exception as e:
        logger.warning(f"[{symbol}] ⚠️ Could not fetch spread: {e}. Using default 0.0001")
        return 0.0001

# === CLASSE PRINCIPALE ===
class UltraAgressiveTrailingStop:
    def __init__(self, client, symbol, min_trail=0.007, max_trail=0.06, smoothing_alpha=0.3, min_profit_activate=0.003):
        self.client = client
        self.symbol = symbol
        self.min_trail = min_trail
        self.max_trail = max_trail
        self.smoothing_alpha = smoothing_alpha
        self.min_profit_activate = min_profit_activate
        self.max_retries = 3
        self.trailing_stop_order_id = None
        self.entry_price = 0.0
        self.stop_loss_price = 0.0
        self.position_type = None
        self.quantity = 0.0
        self.trade_id = None
        self.atr_smooth = None
        self.active = False
        self.rules = get_exchange_precision(client, symbol)
        self.price_precision = self.rules['price_precision']
        self.qty_precision = self.rules['qty_precision']
        self.price_tick = self.rules['price_tick']

    def _generate_client_order_id(self):
        millis = int(time.time() * 1000)
        return f"TS{millis}{random.randint(1000,9999)}"

    def _check_position(self):
        try:
            positions = self.client.get_position_risk(symbol=self.symbol)
            for pos in positions:
                if pos['symbol'] == self.symbol and float(pos['positionAmt']) != 0:
                    return True, float(pos['positionAmt'])
            return False, 0.0
        except Exception as e:
            logger.error(f"[{self.symbol}] ❌ Failed to check position: {e}")
            return False, 0.0

    def _update_smoothed_atr(self, new_atr):
        if self.atr_smooth is None:
            self.atr_smooth = new_atr
        else:
            self.atr_smooth = self.smoothing_alpha * new_atr + (1 - self.smoothing_alpha) * self.atr_smooth
        return self.atr_smooth

    def _calculate_trail_distance(self, current_price, atr, spread, adx):
        if atr is None or atr <= 0 or current_price <= 0:
            logger.warning(f"[{self.symbol}] ⚠️ Invalid ATR or price for trailing stop calculation.")
            return self.min_trail
        atr_smooth = self._update_smoothed_atr(atr)
        base_trail = max(
            (atr_smooth / current_price) * 1.5,
            spread / current_price + 0.5 * atr_smooth / current_price
        )
        adx_factor = max(0.2, min(1.0, 1.2 - (adx / 100)))
        trail = max(self.min_trail, min(base_trail * adx_factor, self.max_trail))
        return trail

    def _initial_stop(self, entry_price, side, trail):
        if side == "long":
            return format_price(self.client, self.symbol, entry_price * (1 - trail))
        else:
            return format_price(self.client, self.symbol, entry_price * (1 + trail))

    def initialize(self, entry_price, position_type, quantity, atr, adx, trade_id):
        self.entry_price = float(entry_price)
        self.position_type = position_type.lower()
        self.quantity = format_quantity(self.client, self.symbol, quantity)
        self.trade_id = str(trade_id).replace('trade_', '')
        self.active = True

        if self.entry_price <= 0:
            logger.error(f"[{self.symbol}] ❌ Invalid entry_price: {self.entry_price}")
            self.active = False
            return None

        has_position, _ = self._check_position()
        if not has_position:
            logger.warning(f"[{self.symbol}] ⚠️ No active position found for trade_id {self.trade_id}. Skipping trailing stop.")
            self.active = False
            return None

        ticker = self.client.ticker_price(symbol=self.symbol)
        current_price = float(ticker['price'])
        if current_price <= 0:
            logger.error(f"[{self.symbol}] ❌ Invalid current_price: {current_price}")
            self.active = False
            return None

        spread = get_spread(self.client, self.symbol)
        trail = self._calculate_trail_distance(current_price, atr, spread, adx)
        self.stop_loss_price = self._initial_stop(self.entry_price, self.position_type, trail)

        for attempt in range(self.max_retries):
            try:
                order = self.client.new_order(
                    symbol=self.symbol,
                    side='SELL' if self.position_type == 'long' else 'BUY',
                    type='STOP_MARKET',
                    quantity=str(self.quantity),
                    stopPrice=str(self.stop_loss_price),
                    priceProtect=True,
                    reduceOnly=True,
                    newClientOrderId=self._generate_client_order_id()
                )
                self.trailing_stop_order_id = order['orderId']
                logger.info(f"[{self.symbol}] ✅ Initial stop placed at {self.stop_loss_price}, trade_id: {self.trade_id}")
                return self.trailing_stop_order_id
            except BinanceAPIException as e:
                if e.code in [-2021, -2019, -2022]:
                    logger.warning(f"[{self.symbol}] ⚠️ Attempt {attempt+1}/{self.max_retries}: {e.message}")
                    time.sleep(1 + attempt)
                else:
                    logger.error(f"[{self.symbol}] ❌ Failed to place initial stop-loss: {e}")
                    self.active = False
                    return None
        self.active = False
        return None

    def update(self, current_price, atr, spread, adx, trade_id=None):
        if not self.active or not self.trailing_stop_order_id:
            return
        trade_id = str(trade_id or '').replace('trade_', '')
        if trade_id and trade_id != self.trade_id:
            return
        has_position, _ = self._check_position()
        if not has_position:
            self.close_position()
            return

        profit_pct = (
            (current_price - self.entry_price) / self.entry_price if self.position_type == "long"
            else (self.entry_price - current_price) / self.entry_price
        )
        if profit_pct < self.min_profit_activate:
            logger.debug(f"[{self.symbol}] Gain insuffisant ({profit_pct*100:.2f}%), pas d'activation.")
            return

        trail = self._calculate_trail_distance(current_price, atr, spread, adx)
        if self.position_type == "long":
            new_stop = max(self.stop_loss_price, current_price * (1 - trail))
        else:
            new_stop = min(self.stop_loss_price, current_price * (1 + trail))

        new_stop = format_price(self.client, self.symbol, new_stop)
        should_update = (
            (self.position_type == "long" and new_stop >= self.stop_loss_price + self.price_tick) or
            (self.position_type == "short" and new_stop <= self.stop_loss_price - self.price_tick)
        )

        if should_update:
            try:
                self.client.cancel_order(symbol=self.symbol, orderId=self.trailing_stop_order_id)
                order = self.client.new_order(
                    symbol=self.symbol,
                    side='SELL' if self.position_type == "long" else 'BUY',
                    type='STOP_MARKET',
                    quantity=str(self.quantity),
                    stopPrice=str(new_stop),
                    priceProtect=True,
                    reduceOnly=True,
                    newClientOrderId=self._generate_client_order_id()
                )
                self.trailing_stop_order_id = order['orderId']
                self.stop_loss_price = new_stop
                logger.info(f"[{self.symbol}] ✅ Trailing stop updated to {new_stop}, trade_id: {self.trade_id}")
                send_telegram_alert(f"Trailing stop updated for {self.symbol} to {new_stop}, trade_id: {self.trade_id}")
            except Exception as e:
                logger.error(f"[{self.symbol}] ❌ Failed to update trailing stop: {e}")

    def close_position(self):
        try:
            if self.trailing_stop_order_id:
                self.client.cancel_order(symbol=self.symbol, orderId=self.trailing_stop_order_id)
        except Exception as e:
            logger.error(f"[{self.symbol}] ❌ Failed to cancel trailing stop: {e}")
        self.active = False
        self.trailing_stop_order_id = None
        self.stop_loss_price = 0.0
        logger.info(f"[{self.symbol}] ✅ Trailing stop removed for trade_id: {self.trade_id}")
        update_trade_on_close(self.trade_id, self.symbol, connection_pool)

# === MANAGER ===
class TrailingStopManager:
    def __init__(self, client):
        self.client = client
        self.stops = {}

    def has_trailing_stop(self, symbol, trade_id=None):
        """Retourne True si un trailing stop actif existe.
        Si trade_id est fourni, vérifie aussi la correspondance du trade_id."""
        if symbol not in self.stops or not self.stops[symbol].active:
            return False
        if trade_id is None:
            return True
        return self.stops[symbol].trade_id == str(trade_id).replace('trade_', '')

    def initialize_trailing_stop(self, symbol, entry_price, position_type, quantity, atr, adx, trade_id):
        if symbol not in self.stops:
            self.stops[symbol] = UltraAgressiveTrailingStop(self.client, symbol)
        return self.stops[symbol].initialize(entry_price, position_type, quantity, atr, adx, trade_id)

    def update_trailing_stop(self, symbol, current_price, atr, spread, adx, trade_id=None):
        if symbol in self.stops and self.stops[symbol].active:
            self.stops[symbol].update(current_price, atr, spread, adx, trade_id)

    def close_position(self, symbol, trade_id=None):
        """Ferme le trailing stop pour ce symbole.
        Si trade_id est fourni, ne ferme que si les ID correspondent."""
        if symbol not in self.stops:
            return
        if trade_id is not None and self.stops[symbol].trade_id != str(trade_id).replace('trade_', ''):
            return
        self.stops[symbol].close_position()
        del self.stops[symbol]
