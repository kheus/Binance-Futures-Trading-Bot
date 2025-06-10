try:
    from binance.enums import SIDE_BUY, SIDE_SELL, ORDER_TYPE_MARKET
except ImportError:
    # Fallback definitions if binance.enums is not available
    SIDE_BUY = 'BUY'
    SIDE_SELL = 'SELL'
    ORDER_TYPE_MARKET = 'MARKET'

import time
import yaml
import logging

logger = logging.getLogger(__name__)

def place_order(signal, price, atr, client, symbol, capital, leverage):
    try:
        if capital <= 0 or price <= 0 or leverage <= 0:
            logger.error(f"[Order Error] Invalid parameters: capital={capital}, price={price}, leverage={leverage}")
            return None
        base_qty = (capital / price) * leverage
        qty = max(round(base_qty / 0.001) * 0.001, 0.001)
        logger.info(f"[Debug] Calculated Qty: base={base_qty}, adjusted={qty}")

        sl = round(price - atr * 1.5 if signal == "buy" else price + atr * 1.5, 2)
        tp = round(price + atr * 3 if signal == "buy" else price - atr * 3, 2)
        logger.info(f"SIGNAL: {signal.upper()} | Price: {price} | Qty: {qty} | SL: {sl} | TP: {tp}")

        try:
            client.change_leverage(symbol=symbol, leverage=leverage)
            logger.info(f"[Leverage] Set to {leverage}x for {symbol}")
        except Exception as e:
            logger.error(f"[Leverage Error] {e}, proceeding without leverage change")
            # Continuer sans lever

        order_side = SIDE_BUY if signal == "buy" else SIDE_SELL
        order = client.new_order(symbol=symbol, side=order_side, type=ORDER_TYPE_MARKET, quantity=qty)
        order_details = {
            "order_id": str(order["orderId"]),
            "symbol": symbol,
            "side": signal,
            "quantity": qty,
            "price": price,
            "stop_loss": sl,
            "take_profit": tp,
            "timestamp": int(time.time() * 1000)
        }
        logger.info(f"[LIVE ORDER] {signal.upper()} {qty} {symbol} at {price}")

        try:
            client.new_order(symbol=symbol, side=order_side, type='STOP_MARKET', stopPrice=sl, quantity=qty)
            client.new_order(symbol=symbol, side=SIDE_SELL if signal == "buy" else SIDE_BUY, type='TAKE_PROFIT_MARKET', stopPrice=tp, quantity=qty)
            logger.info(f"[SL/TP] Placed for {signal.upper()} at SL={sl}, TP={tp}")
        except Exception as e:
            logger.warning(f"[SL/TP Error] {e}, continuing with main order")

        return order_details
    except Exception as e:
        logger.error(f"Order placement error: {e}")
        return None