# File: src/processing_core/indicators.py
from rich.console import Console
from rich.table import Table
import pandas as pd
import numpy as np
import talib
from src.database.db_handler import insert_metrics
import logging

logger = logging.getLogger(__name__)
console = Console()

def calculate_indicators(df, symbol):
    """
    Calculate optimized technical indicators for 15-min timeframe and store metrics.
    """
    try:
        df = df.copy()
        if df.empty or len(df) < 50:
            logger.warning(f"Insufficient data for {symbol}: {len(df)} rows")
            return df

        required_cols = ['open', 'high', 'low', 'close', 'volume']
        df = df.dropna(subset=required_cols)
        if (df[['open', 'high', 'low', 'close']] <= 0).any().any():
            logger.error(f"[Indicators] Invalid price data (zero or negative) for {symbol}")
            return df

        close_prices = df['close'].astype(float).values
        high_prices = df['high'].astype(float).values
        low_prices = df['low'].astype(float).values

        # === Optimized indicator settings for 15min timeframe ===
        with np.errstate(all='ignore'):
            df['RSI'] = talib.RSI(close_prices, timeperiod=9)
            macd, signal, macd_hist = talib.MACD(close_prices, fastperiod=8, slowperiod=21, signalperiod=5)
            df['MACD'] = macd
            df['MACD_signal'] = signal
            df['MACD_hist'] = macd_hist
            df['ADX'] = talib.ADX(high_prices, low_prices, close_prices, timeperiod=10)
            df['EMA9'] = talib.EMA(close_prices, timeperiod=9)
            df['EMA26'] = talib.EMA(close_prices, timeperiod=26)
            df['ATR'] = talib.ATR(high_prices, low_prices, close_prices, timeperiod=10)
            df['ROC'] = talib.ROC(close_prices, timeperiod=6)

        # Clean and validate
        df['ADX'] = np.clip(df['ADX'], 0, 100)
        indicator_cols = ['RSI', 'MACD', 'MACD_signal', 'MACD_hist', 'ADX', 'EMA9', 'EMA26', 'ATR', 'ROC']
        df[indicator_cols] = df[indicator_cols].replace([np.inf, -np.inf], np.nan).ffill().bfill()

        # Store latest metrics
        if not df.empty:
            metrics = {
                "symbol": symbol,
                "timestamp": int(df.index[-1].timestamp() * 1000),
                "rsi": float(df["RSI"].iloc[-1]),
                "macd": float(df["MACD"].iloc[-1]),
                "adx": float(df["ADX"].iloc[-1]),
                "ema9": float(df["EMA9"].iloc[-1]),
                "ema26": float(df["EMA26"].iloc[-1]),
                "atr": float(df["ATR"].iloc[-1]),
                "roc": float(df["ROC"].iloc[-1])
            }
            insert_metrics(symbol, metrics)
            logger.info(f"[Metrics Stored] {metrics}")

        logger.info(f"[Indicators] Calculated successfully for {symbol}")
        return df

    except Exception as e:
        logger.error(f"[Indicators] Error for {symbol}: {e}", exc_info=True)
        return df