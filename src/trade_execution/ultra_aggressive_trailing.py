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

def format_quantity(client, symbol, quantity):
    try:
        rules = get_exchange_precision(client, symbol)
        return round(float(quantity), rules['qty_precision'])
    except Exception as e:
        logger.warning(f"[{symbol}] ⚠️ Could not fetch quantity precision: {e}. Using default precision.")
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

class UltraAgressiveTrailingStop:
    def __init__(self, client, symbol, trailing_distance_range=(0.002, 0.03), smoothing_alpha=0.3):
        self.client = client
        self.symbol = symbol
        self.trailing_distance_range = trailing_distance_range  # (min, max) adjustable range
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
        self.initial_stop_loss = 0.02  # 2% initial stop-loss

        # Variables pour smoothing ATR
        self.smoothed_atr = None
        self.smoothing_alpha = smoothing_alpha  # coefficient EMA

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
        """Generate unique client order ID under 36 characters"""
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
            self.smoothed_atr = (
                self.smoothing_alpha * new_atr + (1 - self.smoothing_alpha) * self.smoothed_atr
            )
        return self.smoothed_atr

    def _compute_trailing_distance(self, atr, adx, current_price):
        if atr is None or atr <= 0 or current_price <= 0:
            logger.warning(f"[{self.symbol}] ⚠️ ATR ou prix invalide pour le calcul du trailing stop.")
            return self.trailing_distance_range[0]  # valeur par défaut (min)

        atr_smooth = self._update_smoothed_atr(atr)
        spread = get_spread(self.client, self.symbol)
        base_trail = max((atr_smooth / current_price) * 1.5, spread / current_price + 0.5 * atr_smooth / current_price)
        adx_factor = max(0.2, min(1.0, 1.2 - (adx / 100)))  # entre 0.2 et 1.0
        trail = base_trail * adx_factor
        trail = max(self.trailing_distance_range[0], min(trail, self.trailing_distance_range[1]))
        logger.debug(
            f"[{self.symbol}] ATR={atr:.5f}, ATR_Smooth={atr_smooth:.5f}, ADX={adx:.2f}, "
            f"Spread={spread:.5f}, Base={base_trail:.5f}, ADX_Factor={adx_factor:.2f}, Final Trailing Distance={trail:.5f}"
        )
        return trail

    def initialize_trailing_stop(self, entry_price, position_type, quantity, atr, adx, trade_id):
        self.entry_price = float(entry_price)
        self.position_type = position_type
        self.quantity = format_quantity(self.client, self.symbol, float(quantity))
        self.trade_id = str(trade_id).replace('trade_', '')
        self.active = True

        if self.entry_price <= 0:
            logger.error(f"[{self.symbol}] ❌ Invalid entry_price: {self.entry_price}")
            return None

        has_position, position_qty = self._check_position()
        if not has_position:
            logger.warning(f"[{self.symbol}] ⚠️ No active position found for trade_id {self.trade_id}. Skipping trailing stop.")
            self.active = False
            return None

        try:
            ticker = self.client.ticker_price(symbol=self.symbol)
            current_price = float(ticker['price'])
        except BinanceAPIException as e:
            logger.error(f"[{self.symbol}] ❌ Failed to fetch current price: {e}")
            return None

        if current_price <= 0:
            logger.error(f"[{self.symbol}] ❌ Invalid current_price: {current_price}")
            return None

        trailing_dist = self._compute_trailing_distance(atr, adx, current_price)
        initial_stop_dist = self.initial_stop_loss  # 2% initial stop-loss

        if self.position_type == 'long':
            stop_price = current_price * (1 - initial_stop_dist)  # Initial stop-loss
            self.current_stop_price = stop_price
        else:
            stop_price = current_price * (1 + initial_stop_dist)
            self.current_stop_price = stop_price

        try:
            account = self.client.account()
            usdt_balance = float(next((x['availableBalance'] for x in account['assets'] if x['asset'] == 'USDT'), 0))
            logger.debug(f"[{self.symbol}] Fetched USDT available balance: {usdt_balance}")
        except BinanceAPIException as e:
            logger.error(f"[{self.symbol}] ❌ Could not fetch account balance: {e}")
            usdt_balance = 0

        notional_value = stop_price * self.quantity
        if notional_value > usdt_balance * 50:
            logger.warning(f"[{self.symbol}] ❌ Insufficient margin (need ≈ {notional_value:.2f}, have ≈ {usdt_balance:.2f})")
            send_telegram_alert(f"Insufficient margin for {self.symbol} trailing stop: need ≈ {notional_value:.2f}, have ≈ {usdt_balance:.2f}")
            return None

        for attempt in range(self.max_retries):
            try:
                client_order_id = self._generate_client_order_id()
                order = self.client.new_order(
                    symbol=self.symbol,
                    side='SELL' if self.position_type == 'long' else 'BUY',
                    type='STOP_MARKET',
                    quantity=str(self.quantity),
                    stopPrice=str(stop_price),
                    priceProtect=True,
                    reduceOnly=True,
                    newClientOrderId=client_order_id
                )
                self.trailing_stop_order_id = order['orderId']
                logger.info(f"[{self.symbol}] ✅ Initial stop-loss placed (order {self.trailing_stop_order_id}) at {stop_price} "
                            f"(Qty: {self.quantity}, trade_id: {self.trade_id}, trailing_dist: {trailing_dist:.6f})")
                return self.trailing_stop_order_id
            except BinanceAPIException as e:
                if e.code == -2021:
                    logger.warning(f"[{self.symbol}] ⚠️ Retry {attempt+1}/{self.max_retries}: Stop price {stop_price} would trigger immediately. Adjusting.")
                    if self.position_type == 'long':
                        stop_price = stop_price * (1 - self.price_tick / current_price)
                    else:
                        stop_price = stop_price * (1 + self.price_tick / current_price)
                    stop_price = round(stop_price, self.price_precision)
                    time.sleep(1 + attempt)
                else:
                    logger.error(f"[{self.symbol}] ❌ Failed to place initial stop-loss: {e}")
                    return None
            except Exception as e:
                logger.error(f"[{self.symbol}] ❌ Retry {attempt+1}/{self.max_retries} failed: {e}")
                time.sleep(1 + attempt)

        logger.error(f"[{self.symbol}] ❌ Failed to place initial stop-loss after {self.max_retries} attempts")
        return None

    def update_trailing_stop(self, current_price, trade_id=None):
        if not self.active or not self.trailing_stop_order_id or not self.entry_price:
            logger.error(f"[{self.symbol}] ❌ No active trailing stop or invalid entry_price")
            return

        trade_id = str(trade_id or '').replace('trade_', '')
        if trade_id and trade_id != self.trade_id:
            logger.warning(f"[{self.symbol}] ⚠️ Trade ID mismatch: expected {self.trade_id}, got {trade_id}")
            return

        has_position, position_qty = self._check_position()
        if not has_position:
            logger.warning(f"[{self.symbol}] ⚠️ No active position found for trade_id {self.trade_id}. Closing trailing stop.")
            self.close_position()
            return

        try:
            current_price = float(current_price)
            if current_price <= 0:
                logger.error(f"[{self.symbol}] ❌ Invalid current_price: {current_price}")
                return

            atr = get_current_atr(self.client, self.symbol)
            adx = get_current_adx(self.client, self.symbol)
            trailing_dist = self._compute_trailing_distance(atr, adx, current_price)

            if self.position_type == 'long':
                new_stop = current_price * (1 - trailing_dist)
            else:
                new_stop = current_price * (1 + trailing_dist)
            new_stop = round(new_stop, self.price_precision)

            should_update = (
                (self.position_type == 'long' and new_stop >= self.current_stop_price + self.price_tick) or
                (self.position_type == 'short' and new_stop <= self.current_stop_price - self.price_tick)
            )

            logger.debug(f"[{self.symbol}] Update check: new_stop={new_stop}, current_stop={self.current_stop_price}, "
                         f"should_update={should_update}, trailing_dist={trailing_dist:.6f}")

            if should_update:
                for attempt in range(self.max_retries):
                    try:
                        self.client.cancel_order(symbol=self.symbol, orderId=self.trailing_stop_order_id)
                        logger.info(f"[{self.symbol}] ❌ Canceled old trailing stop order {self.trailing_stop_order_id}")

                        client_order_id = self._generate_client_order_id()
                        order = self.client.new_order(
                            symbol=self.symbol,
                            side='SELL' if self.position_type == 'long' else 'BUY',
                            type='STOP_MARKET',
                            quantity=str(self.quantity),
                            stopPrice=str(new_stop),
                            priceProtect=True,
                            reduceOnly=True,
                            newClientOrderId=client_order_id
                        )
                        self.trailing_stop_order_id = order['orderId']
                        self.current_stop_price = new_stop
                        logger.info(f"[{self.symbol}] 🔄 Trailing stop updated to {new_stop} (order {self.trailing_stop_order_id}, trade_id: {self.trade_id})")
                        send_telegram_alert(f"Trailing stop updated for {self.symbol} to {new_stop:.4f}, trade_id: {self.trade_id}")
                        return
                    except BinanceAPIException as e:
                        if e.code == -2021:
                            logger.warning(f"[{self.symbol}] ⚠️ Retry {attempt+1}/{self.max_retries}: Stop price {new_stop} would trigger immediately. Adjusting.")
                            if self.position_type == 'long':
                                new_stop = new_stop * (1 - self.price_tick / current_price)
                            else:
                                new_stop = new_stop * (1 + self.price_tick / current_price)
                            new_stop = round(new_stop, self.price_precision)
                            time.sleep(1 + attempt)
                        else:
                            logger.error(f"[{self.symbol}] ❌ Failed to update trailing stop: {e}")
                            return
                    except Exception as e:
                        logger.error(f"[{self.symbol}] ❌ Retry {attempt+1}/{self.max_retries} failed to update trailing stop: {e}")
                        time.sleep(1 + attempt)
            else:
                logger.debug(f"[{self.symbol}] No trailing stop update needed")

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
                pnl = (exit_price - self.entry_price) * self.quantity if self.position_type == 'long' else (self.entry_price - exit_price) * self.quantity

                success = update_trade_on_close(
                    symbol=self.symbol,
                    order_id=self.trade_id,
                    exit_price=exit_price,
                    quantity=self.quantity,
                    side='BUY' if self.position_type == 'long' else 'SELL',
                    leverage=50
                )

                if success:
                    logger.info(f"[{self.symbol}] ✅ Trailing stop order {self.trailing_stop_order_id} executed at {exit_price}, PNL: {pnl:.4f}")
                    send_telegram_alert(f"Trailing stop executed for {self.symbol} at {exit_price:.4f}, PNL: {pnl:.4f}, trade_id: {self.trade_id}")
                    self.close_position()
                    return True
                else:
                    logger.error(f"[{self.symbol}] ❌ Failed to update trade on close for trade_id {self.trade_id}")
                    return False

            elif status in ['CANCELED', 'REJECTED', 'EXPIRED']:
                logger.warning(f"[{self.symbol}] ⚠️ Trailing stop order {self.trailing_stop_order_id} {status.lower()}")
                self.close_position()
                return False

            return False

        except BinanceAPIException as e:
            if e.code == -2011:
                logger.warning(f"[{self.symbol}] ⚠️ Order not found, checking fallback logic (likely filled)")
                has_position, _ = self._check_position()
                if not has_position:
                    logger.info(f"[{self.symbol}] ✅ Position closed on Binance; confirming trailing execution for trade_id {self.trade_id}")
                    update_trade_on_close(
                        symbol=self.symbol,
                        order_id=self.trade_id,
                        exit_price=self.current_stop_price,
                        quantity=self.quantity,
                        side='BUY' if self.position_type == 'long' else 'SELL',
                        leverage=50
                    )
                    send_telegram_alert(f"Trailing stop likely executed for {self.symbol} at {self.current_stop_price:.4f} (order missing but position closed)")
                    self.close_position()
                    return True
            logger.error(f"[{self.symbol}] ❌ Failed to verify order execution: {e}")
            return False

        except Exception as e:
            logger.error(f"[{self.symbol}] ❌ Unexpected error in verify_order_execution: {e}")
            return False

    def close_position(self):
        if self.active:
            try:
                if self.trailing_stop_order_id:
                    self.client.cancel_order(symbol=self.symbol, orderId=self.trailing_stop_order_id)
                    logger.info(f"[{self.symbol}] ✅ Trailing stop order {self.trailing_stop_order_id} canceled")
            except Exception as e:
                logger.error(f"[{self.symbol}] ❌ Failed to cancel trailing stop: {e}")
            self.active = False
            self.trailing_stop_order_id = None
            self.current_stop_price = 0.0
            logger.info(f"[{self.symbol}] ✅ Position closed and trailing stop removed for trade_id: {self.trade_id}")

    def adjust_quantity(self, new_quantity, trade_id=None):
        """Adjust the quantity of the trailing stop order."""
        trade_id = str(trade_id or '').replace('trade_', '')
        if trade_id and trade_id != self.trade_id:
            logger.warning(f"[{self.symbol}] ⚠️ Trade ID mismatch: expected {self.trade_id}, got {trade_id}")
            return

        if not self.active or not self.trailing_stop_order_id:
            logger.error(f"[{self.symbol}] ❌ No active trailing stop to adjust")
            return

        new_quantity = format_quantity(self.client, self.symbol, float(new_quantity))
        if new_quantity <= 0:
            logger.error(f"[{self.symbol}] ❌ Invalid new quantity: {new_quantity}")
            return

        try:
            # Cancel existing stop order
            self.client.cancel_order(symbol=self.symbol, orderId=self.trailing_stop_order_id)
            logger.info(f"[{self.symbol}] ❌ Old trailing stop order {self.trailing_stop_order_id} canceled for quantity adjustment")

            # Place new stop order with updated quantity
            client_order_id = self._generate_client_order_id()
            order = self.client.new_order(
                symbol=self.symbol,
                side='SELL' if self.position_type == 'long' else 'BUY',
                type='STOP_MARKET',
                quantity=str(new_quantity),
                stopPrice=str(self.current_stop_price),
                priceProtect=True,
                reduceOnly=True,
                newClientOrderId=client_order_id
            )
            self.trailing_stop_order_id = order['orderId']
            self.quantity = new_quantity
            logger.info(f"[{self.symbol}] ✅ Trailing stop quantity adjusted to {new_quantity} (order {self.trailing_stop_order_id}, trade_id: {self.trade_id})")
        except Exception as e:
            logger.error(f"[{self.symbol}] ❌ Failed to adjust trailing stop quantity: {e}")

class TrailingStopManager:
    def __init__(self, client):
        self.client = client
        self.stops = {}

    def has_trailing_stop(self, symbol):
        return symbol in self.stops and self.stops[symbol].active and self.stops[symbol].trailing_stop_order_id is not None

    def initialize_trailing_stop(self, symbol, entry_price, position_type, quantity, atr, adx, trade_id):
        if symbol not in self.stops:
            self.stops[symbol] = UltraAgressiveTrailingStop(self.client, symbol)
        return self.stops[symbol].initialize_trailing_stop(entry_price, position_type, quantity, atr, adx, trade_id)

    def update_trailing_stop(self, symbol, current_price, trade_id=None):
        if symbol in self.stops and self.stops[symbol].active:
            self.stops[symbol].update_trailing_stop(current_price, trade_id=trade_id)
            if self.stops[symbol].verify_order_execution():
                self.close_position(symbol)

    def adjust_quantity(self, symbol, new_quantity, trade_id=None):
        if symbol in self.stops and self.stops[symbol].active:
            self.stops[symbol].adjust_quantity(new_quantity, trade_id=trade_id)

    def close_position(self, symbol):
        if symbol in self.stops and self.stops[symbol].active:
            self.stops[symbol].close_position()
            del self.stops[symbol]
            logger.info(f"[{symbol}] ✅ Trailing stop manager closed position")

    def get_current_stop_price(self, symbol, trade_id=None):
        if symbol in self.stops and self.stops[symbol].active:
            return self.stops[symbol].current_stop_price
        return None

def init_trailing_stop_manager(client):
    return TrailingStopManager(client)

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
        return round(float(quantity), rules['qty_precision'])
    except Exception as e:
        logger.warning(f"[{symbol}] ⚠️ Could not fetch quantity precision: {e}. Using default precision.")
        if symbol in ['BTCUSDT', 'ETHUSDT']:
            return round(float(quantity), 3)
        return round(float(quantity), 2)