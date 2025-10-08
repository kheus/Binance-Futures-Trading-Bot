# src/trade_execution/ultra_aggressive_trailing.py
import logging
import time
import random
from binance.um_futures import UMFutures
from binance.exceptions import BinanceAPIException
from src.database.db_handler import update_trade_on_close, connection_pool
from src.monitoring.metrics import get_current_atr, get_current_adx
from src.monitoring.alerting import send_telegram_alert

logger = logging.getLogger(__name__)

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
            'price_precision': symbol_info.get('pricePrecision', 4),
            'qty_precision': symbol_info.get('quantityPrecision', 0)
        }
    except Exception as e:
        logger.error(f"Error getting precision for {symbol}: {e}")
        # Provide sensible defaults
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
        qty = float(quantity)
        # Round according to qty_precision but keep conservative rounding down
        prec = int(rules.get('qty_precision', 0))
        factor = 10 ** prec
        qty = int(qty * factor) / float(factor)
        # symbol specific fallback
        if symbol in ['BTCUSDT', 'ETHUSDT'] and prec == 0:
            qty = round(qty, 3)
        return qty
    except Exception as e:
        logger.warning(f"[{symbol}] ⚠️ Could not fetch quantity precision: {e}. Using default precision.")
        if symbol in ['BTCUSDT', 'ETHUSDT']:
            return round(float(quantity), 3)
        return round(float(quantity), 2)

def get_spread(client, symbol):
    try:
        # Use depth endpoint for Binance Futures
        book = client.depth(symbol=symbol, limit=5)
        if not book or 'bids' not in book or 'asks' not in book or not book['bids'] or not book['asks']:
            raise ValueError("Invalid order book response")
        bid = float(book['bids'][0][0])
        ask = float(book['asks'][0][0])
        spread = ask - bid
        mid_price = (bid + ask) / 2
        spread_pct = (spread / mid_price) * 100 if mid_price > 0 else 0
        logger.debug(f"[{symbol}] Spread: {spread:.8f} ({spread_pct:.4f}%)")
        return spread
    except Exception as e:
        logger.warning(f"[{symbol}] ⚠️ Could not fetch spread: {str(e)}. Using default 0.0001")
        return 0.0001

class UltraAgressiveTrailingStop:
    def __init__(self, client, symbol, fixed_sl_pct=0.05, take_profit_pct=0.08, trail_multiplier=1.5):
        self.client = client
        self.symbol = symbol
        self.fixed_sl_pct = fixed_sl_pct       # Stop loss fixe initial (ex: 0.05 = 5%)
        self.take_profit_pct = take_profit_pct # Take Profit fixe (ex: 0.08 = 8%)
        self.trail_multiplier = trail_multiplier
        self.min_profit_activate = 0.0
        self.profit_lock_margin = 0.0

        self.trailing_stop_order_id = None
        self.entry_price = 0.0
        self.stop_loss_price = 0.0
        self.position_type = None
        self.quantity = 0.0
        self.trade_id = None
        self.active = False
        self.max_retries = 3

    def _generate_client_order_id(self):
        millis = int(time.time() * 1000)
        return f"TS{millis}{random.randint(1000,9999)}"

    def _check_position(self):
        try:
            positions = self.client.get_position_risk(symbol=self.symbol)
            # get_position_risk returns a list; find item
            for pos in positions:
                if pos.get('symbol') == self.symbol and float(pos.get('positionAmt', 0)) != 0:
                    return True, float(pos.get('positionAmt', 0))
            return False, 0.0
        except Exception as e:
            logger.error(f"[{self.symbol}] ❌ Failed to check position: {e}")
            return False, 0.0

    def _initial_stop(self, entry_price, side):
        # For long: stop below entry; for short: stop above entry -> protects against adverse move
        if side == "long":
            return format_price(self.client, self.symbol, entry_price * (1 - self.fixed_sl_pct))
        else:
            return format_price(self.client, self.symbol, entry_price * (1 + self.fixed_sl_pct))

    def _check_take_profit(self, current_price):
        if self.position_type == "long":
            return current_price >= self.entry_price * (1 + self.take_profit_pct)
        else:
            return current_price <= self.entry_price * (1 - self.take_profit_pct)

    def initialize(self, entry_price, position_type, quantity, atr=None, adx=None, trade_id=None):
        """
        Place initial stop-loss (STOP_MARKET reduceOnly) immediately after trade execution.
        Returns orderId or None.
        """
        self.entry_price = float(entry_price)
        self.position_type = position_type.lower()
        self.quantity = format_quantity(self.client, self.symbol, quantity)
        self.trade_id = str(trade_id).replace('trade_', '') if trade_id else None
        self.active = True

        # Stop loss fixe dès l’entrée
        self.stop_loss_price = self._initial_stop(self.entry_price, self.position_type)

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
                # Binance may return orderId under different keys; normalize
                self.trailing_stop_order_id = order.get('orderId') or order.get('orderId', None)
                logger.info(f"[{self.symbol}] ✅ Initial stop placed at {self.stop_loss_price}, trade_id: {self.trade_id}")
                return self.trailing_stop_order_id
            except BinanceAPIException as e:
                logger.warning(f"[{self.symbol}] ⚠️ Attempt {attempt+1}/{self.max_retries} placing initial stop: {e}")
                time.sleep(1 + attempt)
            except Exception as e:
                logger.exception(f"[{self.symbol}] Unexpected error placing initial stop: {e}")
                time.sleep(1 + attempt)

        self.active = False
        logger.error(f"[{self.symbol}] ❌ Failed to place initial stop after {self.max_retries} attempts")
        return None

    def update(self, current_price, atr=None, spread=None, adx=None, trade_id=None):
        """
        Update trailing stop if conditions met.
        Accepts optional atr, spread, adx and trade_id to ensure correct mapping.
        """
        if not self.active or not self.trailing_stop_order_id:
            return

        # normalize trade_id check
        trade_id = str(trade_id or '').replace('trade_', '')
        if trade_id and self.trade_id and trade_id != self.trade_id:
            logger.debug(f"[{self.symbol}] Skip update: incoming trade_id {trade_id} != {self.trade_id}")
            return

        has_position, _ = self._check_position()
        if not has_position:
            logger.info(f"[{self.symbol}] No active position detected -> closing trailing stop object")
            self.close_position()
            return

        # Take Profit check (close immediately if reached)
        if self._check_take_profit(current_price):
            logger.info(f"[{self.symbol}] 🎯 Take-profit hit at {current_price}, closing position")
            self.close_position()
            return

        # compute profit percentage relative to entry
        profit_pct = (
            (current_price - self.entry_price) / self.entry_price if self.position_type == "long"
            else (self.entry_price - current_price) / self.entry_price
        )

        # compute or fallback spread
        if spread is None:
            try:
                spread = get_spread(self.client, self.symbol)
            except Exception:
                spread = 0.0

        # Trailing dynamic calculation using ATR and ADX
        if atr:
            # base trail relative to price
            base_trail = (atr / max(1e-9, self.entry_price))
            # tighten if trend strong (adx high), expand if spread large
            if adx and adx > 40:
                trend_adj = 0.75
            elif adx and adx > 25:
                trend_adj = 0.9
            else:
                trend_adj = 1.0

            spread_adj = 1.0
            # if spread is relatively large compared to price, widen the trailing to avoid hunting
            try:
                spread_pct = spread / max(1e-9, self.entry_price)
                if spread_pct > 0.001:  # arbitrary small threshold
                    spread_adj = min(2.0, 1.0 + spread_pct * 5)
            except Exception:
                spread_adj = 1.0

            trail_pct = max(0.001, self.trail_multiplier * base_trail * trend_adj * spread_adj)
        else:
            trail_pct = 0.05  # fallback fixed

        # Activation threshold: require some profit beyond trail (protect against immediate updates)
        required_profit_to_activate = max(0.02, trail_pct)
        if profit_pct < required_profit_to_activate:
            logger.debug(f"[{self.symbol}] Profit {profit_pct:.4f} < activation {required_profit_to_activate:.4f} -> no update")
            return

        # compute new stop based on side
        if self.position_type == "long":
            candidate_stop = current_price * (1 - trail_pct)
            new_stop = max(self.stop_loss_price, candidate_stop)
            should_update = new_stop >= self.stop_loss_price + get_exchange_precision(self.client, self.symbol)['price_tick']
        else:
            candidate_stop = current_price * (1 + trail_pct)
            new_stop = min(self.stop_loss_price, candidate_stop) if self.stop_loss_price != 0 else candidate_stop
            should_update = new_stop <= self.stop_loss_price - get_exchange_precision(self.client, self.symbol)['price_tick']

        new_stop = format_price(self.client, self.symbol, new_stop)

        if not should_update:
            logger.debug(f"[{self.symbol}] No meaningful stop change (old={self.stop_loss_price}, new={new_stop})")
            return

        # attempt to replace stop order atomically
        try:
            try:
                if self.trailing_stop_order_id:
                    self.client.cancel_order(symbol=self.symbol, orderId=self.trailing_stop_order_id)
            except BinanceAPIException as e:
                # log but continue to try placing a fresh stop (Binance may say order already filled/cancelled)
                logger.debug(f"[{self.symbol}] Warning cancelling old stop: {e}")

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
            self.trailing_stop_order_id = order.get('orderId') or self.trailing_stop_order_id
            self.stop_loss_price = new_stop
            logger.info(f"[{self.symbol}] ✅ Trailing stop updated to {new_stop}, trade_id: {self.trade_id}")
            send_telegram_alert(f"Trailing stop updated for {self.symbol} to {new_stop}, trade_id: {self.trade_id}")
        except BinanceAPIException as e:
            logger.error(f"[{self.symbol}] ❌ Binance API failed updating trailing stop: {e}")
        except Exception as e:
            logger.exception(f"[{self.symbol}] ❌ Unexpected error updating trailing stop: {e}")

    def close_position(self):
        try:
            if self.trailing_stop_order_id:
                try:
                    self.client.cancel_order(symbol=self.symbol, orderId=self.trailing_stop_order_id)
                except BinanceAPIException as e:
                    logger.debug(f"[{self.symbol}] cancel_order warning on close: {e}")
        except Exception as e:
            logger.exception(f"[{self.symbol}] ❌ Failed to cancel trailing stop: {e}")
        self.active = False
        self.trailing_stop_order_id = None
        self.stop_loss_price = 0.0
        logger.info(f"[{self.symbol}] ✅ Trailing stop removed for trade_id: {self.trade_id}")
        if self.trade_id:
            try:
                update_trade_on_close(self.trade_id, self.symbol, connection_pool, self.quantity, self.position_type)
            except Exception as e:
                logger.exception(f"[{self.symbol}] Error updating DB on close: {e}")

class TrailingStopManager:
    def __init__(self, client):
        self.client = client
        # simple map symbol -> UltraAgressiveTrailingStop (one active trade per symbol)
        self.stops = {}

    def has_trailing_stop(self, symbol, trade_id=None):
        if symbol not in self.stops:
            return False
        stop = self.stops[symbol]
        if not stop.active:
            return False
        if trade_id is None:
            return True
        return stop.trade_id == str(trade_id).replace('trade_', '')

    def initialize_trailing_stop(self, symbol, entry_price, position_type, quantity, atr=None, adx=None, trade_id=None):
        if symbol not in self.stops or not self.stops[symbol].active:
            self.stops[symbol] = UltraAgressiveTrailingStop(self.client, symbol)
        return self.stops[symbol].initialize(entry_price, position_type, quantity, atr=atr, adx=adx, trade_id=trade_id)

    def update_trailing_stop(self, symbol, current_price, atr=None, spread=None, adx=None, trade_id=None):
        if symbol in self.stops and self.stops[symbol].active:
            try:
                # Use keyword args to avoid positional mismatches
                self.stops[symbol].update(current_price, atr=atr, spread=spread, adx=adx, trade_id=trade_id)
            except Exception as e:
                logger.exception(f"[{symbol}] ❌ Error in trailing stop update: {e}")
                send_telegram_alert(f"Error in trailing stop updater: {e}")

    def close_position(self, symbol, trade_id=None):
        if symbol not in self.stops:
            return
        if trade_id is not None and self.stops[symbol].trade_id != str(trade_id).replace('trade_', ''):
            return
        try:
            self.stops[symbol].close_position()
        except Exception as e:
            logger.exception(f"[{symbol}] Error closing trailing stop via manager: {e}")
        finally:
            del self.stops[symbol]
            logger.info(f"[{symbol}] ✅ Manager closed position")
