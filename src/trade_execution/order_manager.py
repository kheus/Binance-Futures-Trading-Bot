# Order Manager for Binance Futures Trading

try:
    from binance.enums import SIDE_BUY, SIDE_SELL, ORDER_TYPE_MARKET
except ImportError:
    # Fallback definitions if binance.enums is not available
    SIDE_BUY = 'BUY'
    SIDE_SELL = 'SELL'
    ORDER_TYPE_MARKET = 'MARKET'

import time
import logging
import numpy as np
from src.database.db_handler import insert_trade

logger = logging.getLogger(__name__)

def place_order(signal, price, atr, client, symbol, capital, leverage):
    """
    Place a market order with SL and TP on Binance Futures, and return the order details.
    """

    # === 1. VALIDATION DES PARAMÈTRES ENTRANTS ===
    if signal not in ["buy", "sell"]:
        logger.error(f"[Order Error] Invalid signal: {signal}")
        return None

    if not all(isinstance(x, (int, float)) and x > 0 for x in [capital, price, leverage]):
        logger.error(f"[Order Error] Invalid parameters: capital={capital}, price={price}, leverage={leverage}")
        return None

    try:
        # === 2. CALCUL DE LA QUANTITÉ ADAPTÉE AU TICK SIZE ===
        base_qty = (capital / price) * leverage
        qty = max(round(base_qty / 0.001) * 0.001, 0.001)
        logger.info(f"[Debug] Calculated Qty: base={base_qty}, adjusted={qty}")

        # === 3. CALCUL DU SL ET TP ===
        sl = round(price - atr * 3, 2) if signal == "buy" else round(price + atr * 3, 2)
        tp = round(price + atr * 6, 2) if signal == "buy" else round(price - atr * 6, 2)

        # Protection : distance min de 0.2 %
        min_pct = 0.002
        min_sl_dist = price * min_pct

        if abs(price - sl) < min_sl_dist:
            sl = round(price * (1 - min_pct), 2) if signal == "buy" else round(price * (1 + min_pct), 2)

        if abs(tp - price) < min_sl_dist:
            tp = round(price * (1 + 2 * min_pct), 2) if signal == "buy" else round(price * (1 - 2 * min_pct), 2)

        logger.info(f"[SL/TP Adjusted] Price: {price}, SL: {sl}, TP: {tp}")

        # === 4. MISE EN PLACE DU LEVIER ===
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

        # === 5. PLACEMENT DE L'ORDRE PRINCIPAL (MARKET) ===
        order_side = SIDE_BUY if signal == "buy" else SIDE_SELL
        try:
            order = client.new_order(
                symbol=symbol,
                side=order_side,
                type=ORDER_TYPE_MARKET,
                quantity=qty
            )
        except Exception as e:
            logger.error(f"[Order Error] Failed to place market order: {e}")
            return None

        logger.info(f"[LIVE ORDER] {signal.upper()} {qty} {symbol} at {price}")

        # === 6. PLACEMENT DES ORDRES SL ET TP (STOP_MARKET & TAKE_PROFIT_MARKET) ===
        try:
            opposite_side = SIDE_SELL if signal == "buy" else SIDE_BUY

            # Stop Loss
            client.new_order(
                symbol=symbol,
                side=opposite_side,
                type="STOP_MARKET",
                stopPrice=sl,
                closePosition=True,
                priceProtect=True,
                workingType="MARK_PRICE"
            )

            # Take Profit
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

        except Exception as e:
            logger.warning(f"[SL/TP Error] {e} | SL={sl}, TP={tp} | Continuing with market order only")

        # === 7. CONSTRUCTION DES DÉTAILS D’ORDRE POUR ENREGISTREMENT ===
        order_details = {
            "order_id": str(order["orderId"]),
            "symbol": symbol,
            "side": signal,
            "quantity": qty,
            "price": price,
            "stop_loss": sl,
            "take_profit": tp,
            "timestamp": int(time.time() * 1000),
            "pnl": 0.0  # à mettre à jour à la clôture
        }

        # === 8. ENREGISTREMENT EN BASE DE DONNÉES ===
        insert_trade(order_details)

        return order_details

    except Exception as e:
        logger.error(f"[Fatal Order Error] {e}")
        return None
