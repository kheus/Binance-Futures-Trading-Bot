import pandas as pd

def detect_breakout(df):
    """
    Detecte un breakout à la hausse ou à la baisse sur les 20 dernieres bougies.
    Retourne "breakout_up", "breakout_down" ou None.
    """
    try:
        if len(df) < 21:
            return None
        window_high = df["high"].iloc[-21:-1].max()
        window_low = df["low"].iloc[-21:-1].min()
        close = df["close"].iloc[-1]

        if close > window_high:
            return "breakout_up"
        elif close < window_low:
            return "breakout_down"
        else:
            return None
    except Exception as e:
        print(f"Breakout detection error: {e}")
        return None
