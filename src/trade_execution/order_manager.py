from binance.enums import *
import time
import yaml

def place_order(signal, price, atr, client, symbol, capital, leverage):
    try:
        qty = round((capital * leverage) / price, 5) # Calculate quantity based on capital and leverage
        sl = round(price - atr * 1.5 if signal == "buy" else price + atr * 1.5, 2)
        tp = round(price + atr * 3 if signal == "buy" else price - atr * 3, 2)
        print(f"🔔 SIGNAL: {signal.upper()} | Price: {price} | Qty: {qty} | SL: {sl} | TP: {tp}")
        
        # Enable live trading by uncommenting the following
        client.futures_change_leverage(symbol=symbol, leverage=leverage)
        order_side = SIDE_BUY if signal == "buy" else SIDE_SELL
        order = client.futures_create_order(
            symbol=symbol,
            side=order_side,
            type=ORDER_TYPE_MARKET,
            quantity=qty
        )
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
        print(f"[LIVE ORDER] {signal.upper()} {qty} {symbol} at {price}")
        return order_details
    except Exception as e:
        print(f"Order placement error: {e}")
        return None