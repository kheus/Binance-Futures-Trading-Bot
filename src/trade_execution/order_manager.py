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
from binance.client import Client
import json
from tabulate import tabulate

logger = logging.getLogger(__name__)

def get_tick_info(client, symbol):
    exchange_info = client.exchange_info()
    symbol_info = next((s for s in exchange_info["symbols"] if s["symbol"] == symbol), None)
    if not symbol_info:
        raise ValueError(f"Symbol {symbol} not found in exchange info.")

    price_filter = next(f for f in symbol_info['filters'] if f['filterType'] == 'PRICE_FILTER')
    return float(price_filter['tickSize']), int(symbol_info['quantityPrecision'])

def round_to_tick(value, tick_size):
    return round(round(value / tick_size) * tick_size, 8)

def place_order(signal, price, atr, client, symbol, capital, leverage):
    if signal not in ["buy", "sell"]:
        logger.error(f"[Order Error] Invalid signal: {signal}")
        return None

    if not all(isinstance(x, (int, float)) and x > 0 for x in [capital, price, leverage]):
        logger.error(f"[Order Error] Invalid parameters: capital={capital}, price={price}, leverage={leverage}")
        return None

    try:
        tick_size, quantity_precision = get_tick_info(client, symbol)
        base_qty = capital / price
        qty = round(base_qty * leverage, quantity_precision)

        logger.info(f"[Qty] base={base_qty}, adjusted={qty} ({symbol})")

        sl = price + atr * 2 if signal == "sell" else price - atr * 2
        tp = price - atr * 5 if signal == "sell" else price + atr * 5

        min_gap = price * 0.0025
        if signal == "buy":
            sl = min(sl, price - min_gap)
            tp = max(tp, price + min_gap)
        else:
            sl = max(sl, price + min_gap)
            tp = min(tp, price - min_gap)

        sl = round_to_tick(sl, tick_size)
        tp = round_to_tick(tp, tick_size)

        try:
            client.change_leverage(symbol=symbol, leverage=leverage)
            logger.info(f"[Leverage] {leverage}x for {symbol}")
        except Exception as e:
            logger.error(f"[Leverage Error] {e}, fallback to 1x")
            try:
                client.change_leverage(symbol=symbol, leverage=1)
            except Exception as e2:
                logger.error(f"[Fallback Leverage Error] {e2}")
                return None

        side = SIDE_BUY if signal == "buy" else SIDE_SELL

        try:
            order = client.new_order(
                symbol=symbol,
                side=side,
                type=ORDER_TYPE_MARKET,
                quantity=qty
            )
            logger.info(f"[ORDER] {signal.upper()} {qty} {symbol} @ {price}")
            time.sleep(0.5)
        except Exception as e:
            logger.error(f"[Order Failed] {e}")
            return None

        try:
            positions = client.get_position_risk(symbol=symbol)
            position_amt = float([p for p in positions if p["symbol"] == symbol][0]["positionAmt"])

            if abs(position_amt) > 0:
                opposite = SIDE_SELL if signal == "buy" else SIDE_BUY

                client.new_order(
                    symbol=symbol,
                    side=opposite,
                    type="STOP_MARKET",
                    stopPrice=str(sl),
                    closePosition=True,
                    priceProtect=True,
                    workingType="MARK_PRICE"
                )
                client.new_order(
                    symbol=symbol,
                    side=opposite,
                    type="TAKE_PROFIT_MARKET",
                    stopPrice=str(tp),
                    closePosition=True,
                    priceProtect=True,
                    workingType="MARK_PRICE"
                )
                logger.info(f"[SL/TP] Set for {symbol} | SL={sl}, TP={tp}")
            else:
                logger.warning("No open position â†’ SL/TP skipped")

        except Exception as e:
            logger.warning(f"[SL/TP Error] {e} â†’ Continuing without SL/TP")

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

        # Calculate callback_rate for logging purposes
        callback_rate = max(0.1, min(5.0, round(2 * atr / price * 100, 1)))

        log_data = [
            ["Price", price],
            ["Qty", qty],
            ["SL", sl],
            ["TP", tp],
            ["ATR", atr],
            ["Callback%", callback_rate]
        ]
        logger.info("\n" + tabulate(log_data, headers=["Param", "Value"], tablefmt="fancy_grid"))

        insert_trade(order_details)
        return order_details

    except Exception as e:
        logger.error(f"[Fatal Error] {e}")
        return None

def update_trailing_stop(client, symbol, signal, current_price, atr, base_qty, existing_sl_order_id):
    callback_rate = max(0.1, min(5.0, round(2 * atr / current_price * 100, 1)))
    side = SIDE_SELL if signal == "buy" else SIDE_BUY

    try:
        if existing_sl_order_id is None:
            order = client.new_order(
                symbol=symbol,
                side=side,
                type="TRAILING_STOP_MARKET",
                quantity=base_qty,
                callbackRate=callback_rate,
                timeInForce="GTC"
            )
            logger.info(f"[Trailing Stop] Initialized for {symbol}, new SL order ID: {order['orderId']}, Callback Rate: {callback_rate}%")
            return order["orderId"]
        else:
            client.cancel_order(symbol=symbol, orderId=existing_sl_order_id)
            order = client.new_order(
                symbol=symbol,
                side=side,
                type="TRAILING_STOP_MARKET",
                quantity=base_qty,
                callbackRate=callback_rate,
                timeInForce="GTC"
            )
            logger.info(f"[Trailing Stop] Updated for {symbol}, new SL order ID: {order['orderId']}, Callback Rate: {callback_rate}%")
            return order["orderId"]
    except Exception as e:
        logger.error(f"[Trailing SL Error] Failed to place/update trailing SL for {symbol}: {e}, Params: callbackRate={callback_rate}")
        if existing_sl_order_id is not None:
            return existing_sl_order_id
        else:
            return None

def handle_message(message, symbol, client, signal, atr, base_qty, last_sl_order_id, last_update_price):
    if message.get('e') == 'markPriceUpdate':
        current_price = float(message.get('p', 0))
        if last_update_price is None or abs(current_price - last_update_price) > 0.2 * atr:
            update_trailing_stop(
                client=client,
                symbol=symbol,
                signal=signal,
                current_price=current_price,
                atr=atr,
                base_qty=base_qty,
                existing_sl_order_id=last_sl_order_id
            )
            last_update_price = current_price
        else:
            logger.info(f"[Price Update] No significant price change for {symbol} â†’ Skipping trailing stop update")
    else:
        logger.warning(f"[Trailing Skip] No open position for {symbol} â†’ Skipping trailing stop update")

