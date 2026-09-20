import time
import logging

from binance.um_futures import UMFutures
from binance.exceptions import BinanceAPIException

from src.monitoring.alerting import send_telegram_alert
from src.monitoring.metrics import get_current_atr, get_current_adx
from src.trade_execution.ultra_aggressive_trailing import (
    TrailingStopManager, format_price, format_quantity, get_exchange_precision
)
from src.trade_execution.market_crash_protector import MarketCrashProtector
from src.trade_execution.position_sizing import calculate_risk_based_quantity
from src.database.db_handler import get_db_connection, release_db_connection

logger = logging.getLogger(__name__)

# UltraAgressiveTrailingStop's initial stop is a fixed 5% of entry price (see
# ultra_aggressive_trailing.UltraAgressiveTrailingStop.fixed_sl_pct). Position sizing here
# must use the same distance, or the "risk per trade" the sizing targets won't match the
# stop that's actually placed.
STOP_PCT = 0.05
DEFAULT_RISK_PER_TRADE = 0.01  # fraction of capital risked per trade at the stop

# Shared across the app: a single crash-protection instance so cooldown state (per symbol)
# is consistent regardless of which module checks it.
crash_protector = MarketCrashProtector()

# The trailing-stop manager is created once, from the single Binance client the caller
# (main_bot.py) owns, via init_trailing_stop_manager(). No module-level Binance client is
# created here - a second client silently bound to the wrong network is a bug, not a
# feature, and nothing in this file needs one once entry/close orders go through the
# client that's passed in.
ts_manager = None


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


def close_position_market(client, symbol):
    """Flatten an open position immediately with a reduce-only market order.

    Used as the last line of defense when a protective stop cannot be attached to a
    freshly-opened position: rather than leaving a leveraged position with no stop-loss at
    all, we close it right away and let the signal try again on the next candle.
    """
    try:
        positions = client.get_position_risk(symbol=symbol)
        position = next((p for p in positions if p['symbol'] == symbol and float(p['positionAmt']) != 0), None)
        if not position:
            return True
        amt = float(position['positionAmt'])
        qty = format_quantity(client, symbol, abs(amt))
        side = 'SELL' if amt > 0 else 'BUY'
        client.new_order(
            symbol=symbol,
            side=side,
            type='MARKET',
            quantity=qty,
            reduceOnly=True,
            newClientOrderId=f"emrgclose{int(time.time())}"
        )
        logger.warning(f"[OrderManager] Emergency-flattened {symbol} ({qty} @ market) - no protective stop could be attached")
        send_telegram_alert(f"⚠️ Emergency close: {symbol} position flattened because a protective stop could not be placed")
        return True
    except BinanceAPIException as e:
        logger.critical(f"[OrderManager] CRITICAL: failed to emergency-flatten {symbol}: {e}. Manual intervention required!")
        send_telegram_alert(f"🚨 CRITICAL: could not flatten unprotected {symbol} position - check manually NOW: {e}")
        return False


def close_open_position(client, symbol):
    """Reduce-only market close of whatever quantity is actually open for `symbol`.

    Unlike EnhancedOrderManager.place_enhanced_order (which sizes and opens a *new*
    risk-based position), this closes exactly the live position size reported by Binance,
    so a close signal can't under-close (leaving a residual) or over-close (flipping side).
    Returns an order-details dict compatible with the entry-order shape, or None if there
    was nothing to close or the close failed.
    """
    try:
        positions = client.get_position_risk(symbol=symbol)
        position = next((p for p in positions if p['symbol'] == symbol and float(p['positionAmt']) != 0), None)
        if not position:
            logger.warning(f"[OrderManager] No open position to close for {symbol}")
            return None

        amt = float(position['positionAmt'])
        qty = format_quantity(client, symbol, abs(amt))
        side = 'SELL' if amt > 0 else 'BUY'
        order = client.new_order(
            symbol=symbol,
            side=side,
            type='MARKET',
            quantity=qty,
            reduceOnly=True,
            newClientOrderId=f"close{int(time.time())}"
        )
        avg_price = _get_fill_price(client, symbol, order, fallback_price=float(client.ticker_price(symbol=symbol)['price']))

        return {
            'order_id': str(order['orderId']),
            'symbol': symbol,
            'side': 'sell' if side == 'SELL' else 'buy',
            'quantity': qty,
            'price': avg_price,
            'timestamp': int(order.get('updateTime', order.get('transactTime', 0))),
            'status': order['status'].lower(),
            'pnl': 0.0,
            'stop_loss': None,
            'take_profit': None,
            'is_trailing': False
        }
    except BinanceAPIException as e:
        logger.error(f"[OrderManager] Failed to close position for {symbol}: {e}")
        return None


def _get_fill_price(client, symbol, order, fallback_price, max_checks=3, sleep_seconds=0.5):
    """Best-effort resolution of the actual average fill price for a just-placed MARKET order,
    so the protective stop is measured from where we actually got filled, not the pre-trade
    quote."""
    avg_price = float(order.get('avgPrice') or 0)
    if avg_price > 0:
        return avg_price
    order_id = order.get('orderId')
    for _ in range(max_checks):
        try:
            status = client.get_order(symbol=symbol, orderId=order_id)
            avg_price = float(status.get('avgPrice') or 0)
            if avg_price > 0:
                return avg_price
        except BinanceAPIException as e:
            logger.warning(f"[OrderManager] Could not confirm fill price for {symbol} order {order_id}: {e}")
        time.sleep(sleep_seconds)
    return fallback_price


def place_scaled_take_profits(client, symbol, entry_price, position_type, quantity, trade_id, levels):
    """
    Place plusieurs ordres de take profit échelonnés avec sécurité reduceOnly.
    levels: list of dicts, each with keys 'pct' (float, e.g. 0.01 for +1%), 'fraction' (float, e.g. 0.5 for 50%)
    """
    try:
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

            client.new_order(
                symbol=symbol,
                side="SELL" if position_type == "long" else "BUY",
                type="TAKE_PROFIT_MARKET",
                stopPrice=str(tp_price),
                quantity=str(partial_qty),
                priceProtect=True,
                reduceOnly=True,
                newClientOrderId=f"tp_{symbol}_{trade_id}_{int(time.time())}"
            )

            logger.info(f"[{symbol}] ✅ Scaled TP placed at {tp_price} for {partial_qty}, trade_id: {trade_id}")

    except Exception as e:
        logger.error(f"[{symbol}] ❌ Failed to place scaled take-profits: {e}")


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

    def place_enhanced_order(self, action, symbol, capital, leverage, trade_id,
                              risk_per_trade=DEFAULT_RISK_PER_TRADE):
        global ts_manager
        try:
            if crash_protector.is_in_cooldown(symbol):
                logger.warning(f"[EnhancedOrderManager] {symbol} is in post-crash cooldown, skipping new entry")
                return None

            atr = get_current_atr(self.client, symbol)
            adx = get_current_adx(self.client, symbol)

            price = self.get_current_price(symbol)
            if not price:
                logger.error(f"[EnhancedOrderManager] Failed to get current price for {symbol}")
                return None

            rules = get_exchange_precision(self.client, symbol)
            logger.info(f"[EnhancedOrderManager] Precision rules for {symbol}: {rules}")

            # STOP_PCT matches UltraAgressiveTrailingStop's own fixed initial stop distance -
            # sizing must target the stop that will actually be placed.
            stop_distance = price * STOP_PCT
            quantity = calculate_risk_based_quantity(
                capital=capital,
                price=price,
                stop_distance=stop_distance,
                risk_per_trade=risk_per_trade,
                leverage=leverage,
                qty_precision=rules['qty_precision'],
                min_qty=rules['min_qty']
            )
            if quantity <= 0:
                logger.error(f"[EnhancedOrderManager] Risk-based sizing produced no valid quantity for {symbol} (capital={capital}, price={price})")
                return None

            price = format_price(self.client, symbol, price)

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

            avg_price = _get_fill_price(self.client, symbol, order, fallback_price=price)

            order_data = {
                'order_id': str(order['orderId']),
                'symbol': symbol,
                'side': action,
                'quantity': quantity,
                'price': avg_price,
                'timestamp': int(order.get('updateTime', order.get('transactTime', 0))),
                'status': order['status'].lower(),
                'trade_id': trade_id,
                'pnl': 0.0,
                'stop_loss': None,
                'take_profit': None,
                'is_trailing': False
            }

            # Atomic protective stop: a filled entry with no attached stop is never left
            # open - if the stop can't be placed, the position is flattened immediately.
            if ts_manager is None:
                ts_manager = init_trailing_stop_manager(self.client)
            position_type = 'long' if action == 'buy' else 'short'
            stop_id = ts_manager.initialize_trailing_stop(
                symbol=symbol,
                entry_price=avg_price,
                position_type=position_type,
                quantity=quantity,
                atr=atr,
                adx=adx,
                trade_id=trade_id,
                leverage=leverage
            )
            if not stop_id:
                logger.error(f"[EnhancedOrderManager] Failed to attach protective stop for {symbol} - flattening position immediately")
                close_position_market(self.client, symbol)
                return None

            order_data['is_trailing'] = True
            order_data['stop_loss'] = ts_manager.stops[symbol].stop_loss_price if symbol in ts_manager.stops else None
            logger.info(f"[EnhancedOrderManager] Order + protective stop placed for {symbol}: {order_data['order_id']}")
            return order_data
        except BinanceAPIException as e:
            logger.error(f"[EnhancedOrderManager] Failed to place {action} order for {symbol}: {e}")
            return None

    def close_open_position(self, symbol):
        return close_open_position(self.client, symbol)

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
