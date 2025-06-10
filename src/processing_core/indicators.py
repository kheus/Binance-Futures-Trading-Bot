import pandas as pd
import talib

def calculate_indicators(df):
    try:
        df = df.copy()
        df["EMA20"] = talib.EMA(df["close"], timeperiod=20)
        df["EMA50"] = talib.EMA(df["close"], timeperiod=50)
        macd, signal, _ = talib.MACD(df["close"], fastperiod=12, slowperiod=26, signalperiod=9)
        df["MACD"] = macd
        df["Signal"] = signal
        df["RSI"] = talib.RSI(df["close"], timeperiod=14)
        df["ADX"] = talib.ADX(df["high"], df["low"], df["close"], timeperiod=14)
        df["ATR"] = talib.ATR(df["high"], df["low"], df["close"], timeperiod=14)
        return df
    except Exception as e:
        print(f"Indicator calculation error: {e}")
        return df
