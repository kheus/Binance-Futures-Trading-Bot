import logging

logger = logging.getLogger(__name__)


def calculate_risk_based_quantity(capital, price, stop_distance, risk_per_trade, leverage,
                                   qty_precision=2, min_qty=0.0):
    """
    Size a position so that a stop hit `stop_distance` (in price units) away from entry loses
    approximately `capital * risk_per_trade`, instead of always deploying the full account
    balance at max leverage on every single trade.

    The result is also capped at the notional a max-leverage allocation of the full capital
    would allow, so a very tight stop can't size the position up to something the account
    can't actually support.
    """
    try:
        if price is None or price <= 0 or stop_distance is None or stop_distance <= 0 or capital is None or capital <= 0:
            logger.warning("[PositionSizing] Invalid inputs: price=%s, stop_distance=%s, capital=%s", price, stop_distance, capital)
            return 0.0

        risk_amount = capital * risk_per_trade
        raw_qty = risk_amount / stop_distance
        max_qty_by_leverage = (capital * leverage) / price
        quantity = min(raw_qty, max_qty_by_leverage)
        quantity = round(quantity, qty_precision)

        if quantity < min_qty:
            logger.info(
                "[PositionSizing] Risk-sized quantity %.8f below exchange min_qty %.8f (risk_amount=%.4f, stop_distance=%.6f) - rejecting trade",
                quantity, min_qty, risk_amount, stop_distance
            )
            return 0.0

        return quantity
    except Exception as e:
        logger.error(f"[PositionSizing] Error computing risk-based quantity: {e}")
        return 0.0
