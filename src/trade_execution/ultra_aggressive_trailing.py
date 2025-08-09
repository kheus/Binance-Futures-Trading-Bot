# File: src/trade_execution/ultra_aggressive_trailing.py
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
        # Exceptions BTC et ETH
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
    def __init__(self, client, symbol, trailing_distance_range=(0.002, 0.03), smoothing_alpha=0.3):
        self.client = client
        self.symbol = symbol
        self.trailing_distance_range = trailing_distance_range
        self.max_retries = 3
        self.trailing_stop_order_id = None
        self.entry_price = 0.0
        self.current_stop_price = 0.0
        self.position_type = None
        self.quantity = 0.0
        self.trade_id = None
        self.price_precision = 4
        self.qty_precision = 2
        self.price_tick = self._get_price_tick()
        self.active = False
        self.initial_stop_loss = 0.02  # sera ajusté dynamiquement
        self.smoothed_atr = None
        self.smoothing_alpha = smoothing_alpha

    def _get_price_tick(self):
        try:
            rules = get_exchange_precision(self.client, self.symbol)
            self.price_precision = rules['price_precision']
            self.qty_precision = rules['qty_precision']
            return rules['price_tick']
        except Exception as e:
            logger.warning(f"[{self.symbol}] ⚠️ Could not fetch price tick: {e}. Using default 0.0001")
            return 0.0001

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
        if self.smoothed_atr is None:
            self.smoothed_atr = new_atr
        else:
            self.smoothed_atr = self.smoothing_alpha * new_atr + (1 - self.smoothing_alpha) * self.smoothed_atr
        return self.smoothed_atr

    def _compute_trailing_distance(self, atr, adx, current_price):
        if atr is None or atr <= 0 or current_price <= 0:
            logger.warning(f"[{self.symbol}] ⚠️ ATR ou prix invalide pour le calcul du trailing stop.")
            return self.trailing_distance_range[0]
        atr_smooth = self._update_smoothed_atr(atr)
        spread = get_spread(self.client, self.symbol)
        base_trail = max((atr_smooth / current_price) * 1.5, spread / current_price + 0.5 * atr_smooth / current_price)
        adx_factor = max(0.2, min(1.0, 1.2 - (adx / 100)))
        trail = max(self.trailing_distance_range[0], min(base_trail * adx_factor, self.trailing_distance_range[1]))
        return trail

    def initialize_trailing_stop(self, entry_price, position_type, quantity, atr, adx, trade_id):
        self.entry_price = float(entry_price)
        self.position_type = position_type if isinstance(position_type, str) else str(position_type.get('side', '')).lower()
        self.quantity = format_quantity(self.client, self.symbol, quantity)
        self.trade_id = str(trade_id).replace('trade_', '')
        self.active = True

        if self.entry_price <= 0:
            logger.error(f"[{self.symbol}] ❌ Invalid entry_price: {self.entry_price}")
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
            return None

        trailing_dist = self._compute_trailing_distance(atr, adx, current_price)
        self.initial_stop_loss = max(self.trailing_distance_range[0], min(atr / current_price, self.trailing_distance_range[1]))

        if self.position_type == 'long':
            stop_price = current_price * (1 - self.initial_stop_loss)
        else:
            stop_price = current_price * (1 + self.initial_stop_loss)

        stop_price = format_price(self.client, self.symbol, stop_price)

        for attempt in range(self.max_retries):
            try:
                order = self.client.new_order(
                    symbol=self.symbol,
                    side='SELL' if self.position_type == 'long' else 'BUY',
                    type='STOP_MARKET',
                    quantity=str(self.quantity),
                    stopPrice=str(stop_price),
                    priceProtect=True,
                    reduceOnly=True,
                    newClientOrderId=self._generate_client_order_id()
                )
                self.trailing_stop_order_id = order['orderId']
                self.current_stop_price = stop_price
                logger.info(f"[{self.symbol}] ✅ Initial stop placed at {stop_price}, trade_id: {self.trade_id}")
                return self.trailing_stop_order_id
            except BinanceAPIException as e:
                if e.code in [-2021, -2019, -2022]:
                    logger.warning(f"[{self.symbol}] ⚠️ Attempt {attempt+1}/{self.max_retries}: {e.message}")
                    time.sleep(1 + attempt)
                else:
                    logger.error(f"[{self.symbol}] ❌ Failed to place initial stop-loss: {e}")
                    return None
        return None

    def update_trailing_stop(self, current_price, trade_id=None):
        if not self.active or not self.trailing_stop_order_id:
            return
        trade_id = str(trade_id or '').replace('trade_', '')
        if trade_id and trade_id != self.trade_id:
            return
        has_position, _ = self._check_position()
        if not has_position:
            self.close_position()
            return

        atr = get_current_atr(self.client, self.symbol)
        adx = get_current_adx(self.client, self.symbol)
        trailing_dist = self._compute_trailing_distance(atr, adx, current_price)

        if self.position_type == 'long':
            new_stop = current_price * (1 - trailing_dist)
        else:
            new_stop = current_price * (1 + trailing_dist)

        new_stop = format_price(self.client, self.symbol, new_stop)
        should_update = (
            (self.position_type == 'long' and new_stop >= self.current_stop_price + self.price_tick) or
            (self.position_type == 'short' and new_stop <= self.current_stop_price - self.price_tick)
        )

        if should_update:
            try:
                self.client.cancel_order(symbol=self.symbol, orderId=self.trailing_stop_order_id)
                order = self.client.new_order(
                    symbol=self.symbol,
                    side='SELL' if self.position_type == 'long' else 'BUY',
                    type='STOP_MARKET',
                    quantity=str(self.quantity),
                    stopPrice=str(new_stop),
                    priceProtect=True,
                    reduceOnly=True,
                    newClientOrderId=self._generate_client_order_id()
                )
                self.trailing_stop_order_id = order['orderId']
                self.current_stop_price = new_stop
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
        self.current_stop_price = 0.0
        logger.info(f"[{self.symbol}] ✅ Trailing stop removed for trade_id: {self.trade_id}")

# === MANAGER ===
class TrailingStopManager:
    def __init__(self, client):
        self.client = client
        self.stops = {}

    def has_trailing_stop(self, symbol):
        return symbol in self.stops and self.stops[symbol].active

    def initialize_trailing_stop(self, symbol, entry_price, position_type, quantity, atr, adx, trade_id):
        if symbol not in self.stops:
            self.stops[symbol] = UltraAgressiveTrailingStop(self.client, symbol)
        return self.stops[symbol].initialize_trailing_stop(entry_price, position_type, quantity, atr, adx, trade_id)

    def update_trailing_stop(self, symbol, current_price, trade_id=None):
        if symbol in self.stops and self.stops[symbol].active:
            self.stops[symbol].update_trailing_stop(current_price, trade_id)

    def close_position(self, symbol):
        if symbol in self.stops:
            self.stops[symbol].close_position()
            del self.stops[symbol]

def init_trailing_stop_manager(client):
    return TrailingStopManager(client)