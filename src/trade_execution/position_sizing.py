import numpy as np
import talib
def get_tick_info(client, symbol):
    """
    Fetches the tick size for a given symbol using the Binance client.
    Returns a tuple (tick_size, step_size).
    """
    info = client.get_symbol_info(symbol)
    for f in info['filters']:
        if f['filterType'] == 'PRICE_FILTER':
            tick_size = float(f['tickSize'])
        if f['filterType'] == 'LOT_SIZE':
            step_size = float(f['stepSize'])
    return tick_size, step_size

def dynamic_position_sizing(client, symbol, capital, risk_per_trade=0.01):
    klines = client.get_historical_klines(symbol, "15m", "24h UTC")
    highs = [float(entry[2]) for entry in klines]
    lows = [float(entry[3]) for entry in klines]
    closes = [float(entry[4]) for entry in klines]
    atr = talib.ATR(np.array(highs), np.array(lows), np.array(closes), timeperiod=14)[-1]
    tick_size = get_tick_info(client, symbol)[0]
    risk_amount = capital * risk_per_trade
    position_size = risk_amount / (atr * 2)
    return round_to_tick(position_size, tick_size)
    
def round_to_tick(value, tick_size):
        """
        Rounds the value to the nearest multiple of tick_size.
        """
        return round(round(value / tick_size) * tick_size, len(str(tick_size).split('.')[-1]))