import pandas as pd
import talib
from src.database.db_handler import insert_metrics
import logging
import time

logger = logging.getLogger(__name__)

def calculate_indicators(df):
    """
    Calculate technical indicators on the DataFrame and store metrics.
    """
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

        # Remplissage des NaN avec des valeurs par defaut
        indicator_cols = ["EMA20", "EMA50", "MACD", "Signal", "RSI", "ADX", "ATR"]
        df[indicator_cols] = df[indicator_cols].ffill().bfill().interpolate()

        # Stockage des metriques pour la derniere ligne
        if not df.empty:
            metrics = {
                "symbol": "BTCUSDT",  # Ajuster si le symbol est dans df
                "timestamp": int(df.index[-1].timestamp() * 1000),  # Utiliser l'index timestamp
                "rsi": float(df["RSI"].iloc[-1]),
                "macd": float(df["MACD"].iloc[-1]),
                "adx": float(df["ADX"].iloc[-1]),
                "ema20": float(df["EMA20"].iloc[-1]),
                "ema50": float(df["EMA50"].iloc[-1]),
                "atr": float(df["ATR"].iloc[-1])
            }
            insert_metrics(metrics)
            logger.info(f"[Metrics Stored] {metrics}")

        return df

    except Exception as e:
        logger.error(f"Indicator calculation error: {e}")
        return df