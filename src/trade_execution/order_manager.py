try:
    from binance.enums import SIDE_BUY, SIDE_SELL, ORDER_TYPE_MARKET
except ImportError:
    SIDE_BUY = 'BUY'
    SIDE_SELL = 'SELL'
    ORDER_TYPE_MARKET = 'MARKET'

import time
import logging
import numpy as np
from src.database.db_handler import insert_trade

logger = logging.getLogger(__name__)

TICK_SIZE = 0.1  # ⚠️ À adapter selon le symbole réel (via exchangeInfo)

def round_to_tick(price, tick_size=TICK_SIZE):
    return round(round(price / tick_size) * tick_size, 2)

def place_order(signal, price, atr, client, symbol, capital, leverage):
    if signal not in ["buy", "sell"]:
        logger.error(f"[Order Error] Invalid signal: {signal}")
        return None

    if not all(isinstance(x, (int, float)) and x > 0 for x in [capital, price, leverage]):
        logger.error(f"[Order Error] Invalid parameters: capital={capital}, price={price}, leverage={leverage}")
        return None

    try:
        base_qty = (capital / price) * leverage
        qty = max(round(base_qty / 0.001) * 0.001, 0.001)
        logger.info(f"[Debug] Calculated Qty: base={base_qty}, adjusted={qty}")

        # SL/TP initiaux
        sl = price + atr * 2 if signal == "sell" else price - atr * 2
        tp = price - atr * 5 if signal == "sell" else price + atr * 5

        # Distance minimale (protection)
        min_gap = price * 0.0025  # 0.25%
        if signal == "buy":
            sl = min(sl, price - min_gap)
            tp = max(tp, price + min_gap)
        else:
            sl = max(sl, price + min_gap)
            tp = min(tp, price - min_gap)

        # Arrondi au tick size
        sl = round_to_tick(sl)
        tp = round_to_tick(tp)

        logger.info(f"[SL/TP Adjusted] Price: {price}, SL: {sl}, TP: {tp}")

        try:
            client.change_leverage(symbol=symbol, leverage=leverage)
            logger.info(f"[Leverage] Set to {leverage}x for {symbol}")
        except Exception as e:
            logger.error(f"[Leverage Error] {e}, falling back to 1x")
            try:
                client.change_leverage(symbol=symbol, leverage=1)
            except Exception as e2:
                logger.error(f"[Fallback Leverage Error] {e2}")
                return None

        order_side = SIDE_BUY if signal == "buy" else SIDE_SELL
        try:
            order = client.new_order(
                symbol=symbol,
                side=order_side,
                type=ORDER_TYPE_MARKET,
                quantity=qty
            )
            logger.info(f"[LIVE ORDER] {signal.upper()} {qty} {symbol} at {price}")
            time.sleep(0.4)
        except Exception as e:
            logger.error(f"[Order Error] Failed to place market order: {e}")
            return None

        try:
            position_info = client.get_position_risk(symbol=symbol)
            position_amt = float([p for p in position_info if p["symbol"] == symbol][0]["positionAmt"])

            if abs(position_amt) > 0:
                opposite_side = SIDE_SELL if signal == "buy" else SIDE_BUY

                client.new_order(
                    symbol=symbol,
                    side=opposite_side,
                    type="STOP_MARKET",
                    stopPrice=sl,
                    closePosition=True,
                    priceProtect=True,
                    workingType="MARK_PRICE"
                )

                client.new_order(
                    symbol=symbol,
                    side=opposite_side,
                    type="TAKE_PROFIT_MARKET",
                    stopPrice=tp,
                    closePosition=True,
                    priceProtect=True,
                    workingType="MARK_PRICE"
                )

                logger.info(f"[SL/TP] Orders placed: SL={sl}, TP={tp}")
            else:
                logger.warning("No position detected, skipping SL/TP orders.")

        except Exception as e:
            logger.warning(f"[SL/TP Error] {e} | SL={sl}, TP={tp} | Continuing with market order only")

        order_details = {
            "order_id": str(order["orderId"]),
            "symbol": symbol,
            "side": signal,
            "quantity": qty,
            "price": price,
            "stop_loss": sl,
            "take_profit": tp,
            "timestamp": int(time.time() * 1000),
            "pnl": 0.0
        }

        insert_trade(order_details)
        return order_details

    except Exception as e:
        logger.error(f"[Fatal Order Error] {e}")
        return None

def update_trailing_stop(client, symbol, signal, current_price, atr, base_qty, existing_sl_order_id):
    """
    Place or update a dynamic trailing stop-loss.
    """
    # Calculate trailing delta (capped at 5000 points, minimum 10 points)
    trailing_delta = max(10, min(5000, int(round(2 * atr / TICK_SIZE) * TICK_SIZE)))
    sl_price = round(current_price - (2 * atr), 2) if signal == "buy" else round(current_price + (2 * atr), 2)
    side = SIDE_SELL if signal == "buy" else SIDE_BUY

    try:
        if existing_sl_order_id is None:
            # Place initial trailing stop order
            order = client.new_order(
                symbol=symbol,
                side=side,
                type="TRAILING_STOP_MARKET",
                quantity=base_qty,
                trailingDelta=trailing_delta,  # Trailing distance in points
                timeInForce="GTC",
                # activationPrice=sl_price  # Optional, remove if causing issues
            )
            logger.info(f"[Trailing Stop] Initialized for {symbol}, new SL order ID: {order['orderId']}, Delta: {trailing_delta}")
            return order["orderId"]
        else:
            # Update existing trailing stop
            client.cancel_order(symbol=symbol, orderId=existing_sl_order_id)
            order = client.new_order(
                symbol=symbol,
                side=side,
                type="TRAILING_STOP_MARKET",
                quantity=base_qty,
                trailingDelta=trailing_delta,
                timeInForce="GTC"
            )
            logger.info(f"[Trailing Stop] Updated for {symbol}, new SL order ID: {order['orderId']}, Delta: {trailing_delta}")
            return order["orderId"]
    except Exception as e:
        logger.error(f"[Trailing SL Error] Failed to place/update trailing SL for {symbol}: {e}, Params: trailingDelta={trailing_delta}, sl_price={sl_price}")
        if existing_sl_order_id is not None:
            return existing_sl_order_id
        else:
            return None
