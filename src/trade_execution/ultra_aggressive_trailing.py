# File: src/trade_execution/ultra_aggressive_trailing.py
import logging
import time
from binance.um_futures import UMFutures
from binance.exceptions import BinanceAPIException
from src.database.db_handler import update_trade_on_close, connection_pool
from src.monitoring.metrics import get_current_atr
from src.monitoring.alerting import send_telegram_alert

logger = logging.getLogger(__name__)

class UltraAgressiveTrailingStop:
    def __init__(self, client, symbol, trailing_distance=0.001):  # 0.1% trailing distance
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
        self.price_precision = 4  # Default precision
        self.qty_precision = 2    # Default precision
        self.price_tick = self._get_price_tick()
        self.active = False  # Indicateur pour suivre si le trailing stop est actif

    def _get_price_tick(self):
        """Fetch price tick size and precision for the symbol from Binance exchange info."""
        try:
            exchange_info = self.client.exchange_info()
            for symbol_info in exchange_info['symbols']:
                if symbol_info['symbol'] == self.symbol:
                    price_filter = next(f for f in symbol_info['filters'] if f['filterType'] == 'PRICE_FILTER')
                    qty_filter = next(f for f in symbol_info['filters'] if f['filterType'] == 'LOT_SIZE')
                    self.price_precision = symbol_info['pricePrecision']
                    self.qty_precision = symbol_info['quantityPrecision']
                    return float(price_filter['tickSize'])
            logger.warning(f"[{self.symbol}] ⚠️ Symbol not found in exchange info. Using default tick size 0.0001")
            return 0.0001
        except Exception as e:
            logger.warning(f"[{self.symbol}] ⚠️ Could not fetch price tick: {e}. Using default 0.0001")
            return 0.0001

    def _check_position(self):
        """Check if an active position exists for the symbol."""
        try:
            positions = self.client.get_position_risk(symbol=self.symbol)
            for pos in positions:
                if pos['symbol'] == self.symbol and float(pos['positionAmt']) != 0:
                    return True, float(pos['positionAmt'])
            return False, 0.0
        except Exception as e:
            logger.error(f"[{self.symbol}] ❌ Failed to check position: {e}")
            return False, 0.0

    def initialize_trailing_stop(self, entry_price, position_type, quantity, atr, trade_id):
        self.entry_price = float(entry_price)
        self.position_type = position_type
        self.quantity = format_quantity(self.symbol, float(quantity))
        self.trade_id = str(trade_id).replace('trade_', '')  # Normalize trade_id
        self.active = True

        if self.entry_price <= 0:
            logger.error(f"[{self.symbol}] ❌ Invalid entry_price: {self.entry_price}")
            return None

        # Verify position exists
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

        # Use ATR-based trailing distance for adaptability
        trailing_distance = max(self.trailing_distance, atr * 2 / current_price)
        stop_price = (
            current_price * (1 - trailing_distance) if position_type == 'long'
            else current_price * (1 + trailing_distance)
        )
        stop_price = round(stop_price, self.price_precision)

        try:
            account = self.client.account()
            usdt_balance = float(next((x['availableBalance'] for x in account['assets'] if x['asset'] == 'USDT'), 0))
            logger.debug(f"[{self.symbol}] Fetched USDT available balance: {usdt_balance}")
        except BinanceAPIException as e:
            logger.error(f"[{self.symbol}] ❌ Could not fetch account balance: {e}")
            usdt_balance = 0
        except Exception as e:
            logger.error(f"[{self.symbol}] ❌ Unexpected error fetching balance: {e}")
            usdt_balance = 0

        notional_value = stop_price * self.quantity
        if notional_value > usdt_balance * 10:  # Adjusted to 10x leverage for testnet
            logger.warning(f"[{self.symbol}] ❌ Insufficient margin (need ≈ {notional_value:.2f}, have ≈ {usdt_balance:.2f})")
            send_telegram_alert(f"Insufficient margin for {self.symbol} trailing stop: need ≈ {notional_value:.2f}, have ≈ {usdt_balance:.2f}")
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
                    reduceOnly=True,
                    newClientOrderId=f"trailing_stop_{self.symbol}_{self.trade_id}"
                )
                self.trailing_stop_order_id = order['orderId']
                self.current_stop_price = stop_price
                logger.info(f"[{self.symbol}] ✅ Trailing stop placed (order {self.trailing_stop_order_id}) at {stop_price} (Qty: {self.quantity}, trade_id: {self.trade_id})")
                send_telegram_alert(f"Trailing stop placed for {self.symbol} at {stop_price:.2f}, Qty: {self.quantity:.4f}, trade_id: {self.trade_id}")
                return self.trailing_stop_order_id
            except BinanceAPIException as e:
                if e.code == -2021:  # Order would immediately trigger
                    logger.warning(f"[{self.symbol}] ⚠️ Retry {attempt+1}/{self.max_retries}: Stop price {stop_price} would trigger immediately. Adjusting.")
                    stop_price = (
                        stop_price * (1 - self.price_tick / current_price) if position_type == 'long'
                        else stop_price * (1 + self.price_tick / current_price)
                    )
                    stop_price = round(stop_price, self.price_precision)
                    time.sleep(1 + attempt)
                else:
                    logger.error(f"[{self.symbol}] ❌ Failed to place trailing stop: {e}")
                    return None
            except Exception as e:
                logger.error(f"[{self.symbol}] ❌ Retry {attempt+1}/{self.max_retries} failed: {e}")
                time.sleep(1 + attempt)
        logger.error(f"[{self.symbol}] ❌ Failed to place trailing stop after {self.max_retries} attempts")
        return None

    def update_trailing_stop(self, current_price, trade_id=None):
        if not self.active or not self.trailing_stop_order_id or not self.entry_price:
            logger.error(f"[{self.symbol}] ❌ No active trailing stop or invalid entry_price")
            return

        trade_id = str(trade_id or '').replace('trade_', '')  # Normalize trade_id
        if trade_id and trade_id != self.trade_id:
            logger.warning(f"[{self.symbol}] ⚠️ Trade ID mismatch: expected {self.trade_id}, got {trade_id}")
            return

        # Verify position still exists
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
            trailing_distance = max(self.trailing_distance, atr * 2 / current_price) if atr else self.trailing_distance
            new_stop = (
                current_price * (1 - trailing_distance) if self.position_type == 'long'
                else current_price * (1 + trailing_distance)
            )
            new_stop = round(new_stop, self.price_precision)

            should_update = (
                (self.position_type == 'long' and new_stop >= self.current_stop_price + self.price_tick) or
                (self.position_type == 'short' and new_stop <= self.current_stop_price - self.price_tick)
            )

            logger.debug(f"[{self.symbol}] Checking update: new_stop={new_stop}, current_stop_price={self.current_stop_price}, "
                        f"should_update={should_update}, position_type={self.position_type}, price_tick={self.price_tick}")

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
                            reduceOnly=True,
                            newClientOrderId=f"trailing_stop_{self.symbol}_{self.trade_id}"
                        )
                        self.trailing_stop_order_id = order['orderId']
                        self.current_stop_price = new_stop
                        logger.info(f"[{self.symbol}] 🔄 Trailing stop updated to {new_stop} (order {self.trailing_stop_order_id}, trade_id: {self.trade_id})")
                        send_telegram_alert(f"Trailing stop updated for {self.symbol} to {new_stop:.2f}, trade_id: {self.trade_id}")
                        return
                    except BinanceAPIException as e:
                        if e.code == -2021:
                            logger.warning(f"[{self.symbol}] ⚠️ Retry {attempt+1}/{self.max_retries}: Stop price {new_stop} would trigger immediately. Adjusting.")
                            new_stop = (
                                new_stop * (1 - self.price_tick / current_price) if self.position_type == 'long'
                                else new_stop * (1 + self.price_tick / current_price)
                            )
                            new_stop = round(new_stop, self.price_precision)
                            time.sleep(1 + attempt)
                        else:
                            logger.error(f"[{self.symbol}] ❌ Failed to update trailing stop: {e}")
                            return
                    except Exception as e:
                        logger.error(f"[{self.symbol}] ❌ Retry {attempt+1}/{self.max_retries} failed to update trailing stop: {e}")
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
                if self.position_type == 'long':
                    pnl = (exit_price - self.entry_price) * self.quantity
                else:
                    pnl = (self.entry_price - exit_price) * self.quantity
                success = update_trade_on_close(
                    symbol=self.symbol,
                    order_id=self.trade_id,
                    exit_price=exit_price,
                    quantity=self.quantity,
                    side='BUY' if self.position_type == 'long' else 'SELL',
                    leverage=50
                )
                if success:
                    logger.info(f"[{self.symbol}] ✅ Trailing stop order {self.trailing_stop_order_id} executed at {exit_price}, trade_id: {self.trade_id}, PNL: {pnl}")
                    send_telegram_alert(f"Trailing stop executed for {self.symbol} at {exit_price:.2f}, PNL: {pnl:.2f}, trade_id: {self.trade_id}")
                    self.close_position()
                    return True
                else:
                    logger.error(f"[{self.symbol}] ❌ Failed to update trade for trade_id {self.trade_id}")
                    return False
            elif status in ['CANCELED', 'REJECTED', 'EXPIRED']:
                logger.warning(f"[{self.symbol}] ⚠️ Trailing stop order {self.trailing_stop_order_id} {status.lower()}")
                self.close_position()
                return False
            return False
        except Exception as e:
            logger.error(f"[{self.symbol}] ❌ Failed to verify order execution: {e}")
            return False

    def close_position(self):
        """Ferme la position et supprime le trailing stop."""
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

        new_quantity = format_quantity(self.symbol, float(new_quantity))
        if new_quantity <= 0:
            logger.error(f"[{self.symbol}] ❌ Invalid new quantity: {new_quantity}")
            return

        try:
            # Cancel existing stop order
            self.client.cancel_order(symbol=self.symbol, orderId=self.trailing_stop_order_id)
            logger.info(f"[{self.symbol}] ❌ Old trailing stop order {self.trailing_stop_order_id} canceled for quantity adjustment")

            # Place new stop order with updated quantity
            order = self.client.new_order(
                symbol=self.symbol,
                side='SELL' if self.position_type == 'long' else 'BUY',
                type='STOP_MARKET',
                quantity=str(new_quantity),
                stopPrice=str(self.current_stop_price),
                priceProtect=True,
                reduceOnly=True,
                newClientOrderId=f"trailing_stop_{self.symbol}_{self.trade_id}"
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

    def initialize_trailing_stop(self, symbol, entry_price, position_type, quantity, atr, trade_id):
        if symbol not in self.stops:
            self.stops[symbol] = UltraAgressiveTrailingStop(self.client, symbol)
        return self.stops[symbol].initialize_trailing_stop(entry_price, position_type, quantity, atr, trade_id)

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

def format_price(symbol, price):
    if symbol in ['BTCUSDT', 'ETHUSDT']:
        return round(float(price), 2)
    return round(float(price), 4)

def format_quantity(symbol, quantity):
    if symbol in ['BTCUSDT', 'ETHUSDT']:
        return round(float(quantity), 3)
    return round(float(quantity), 2)