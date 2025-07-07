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
    Calculate technical indicators for a given symbol and store metrics.
    """
    try:
        df = df.copy()
        if df.empty or len(df) < 50:
            logger.warning(f"Insufficient data for {symbol}: {len(df)} rows")
            return df

        # Verify required columns and validate price data
        required_cols = ['open', 'high', 'low', 'close', 'volume']
        df = df.dropna(subset=required_cols)
        if (df[['open', 'high', 'low', 'close']] <= 0).any().any():
            logger.error(f"[Indicators] Invalid price data (zero or negative) for {symbol}")
            return df

        # Log input price data for debugging
        logger.debug(f"[Indicators] Price data for {symbol}: "
                     f"High={df['high'].iloc[-5:].tolist()}, "
                     f"Low={df['low'].iloc[-5:].tolist()}, "
                     f"Close={df['close'].iloc[-5:].tolist()}, "
                     f"Rows={len(df)}")

        close_prices = df['close'].values.astype(float)
        high_prices = df['high'].values.astype(float)
        low_prices = df['low'].values.astype(float)

        # Calculate indicators
        with np.errstate(all='ignore'):
            df['RSI'] = talib.RSI(close_prices, timeperiod=14)
            macd, signal, macd_hist = talib.MACD(close_prices, fastperiod=12, slowperiod=26, signalperiod=9)
            df['MACD'] = macd
            df['MACD_signal'] = signal
            df['MACD_hist'] = macd_hist
            df['ADX'] = talib.ADX(high_prices, low_prices, close_prices, timeperiod=14)
            df['EMA20'] = talib.EMA(close_prices, timeperiod=20)
            df['EMA50'] = talib.EMA(close_prices, timeperiod=50)
            df['ATR'] = talib.ATR(high_prices, low_prices, close_prices, timeperiod=14)
            df['ROC'] = talib.ROC(close_prices, timeperiod=5)

        # Cap ADX at 100 and validate outliers
        df['ADX'] = np.clip(df['ADX'], 0, 100)
        if df['ADX'].iloc[-1] > 75:
            logger.warning(f"[Indicators] High ADX for {symbol}: {df['ADX'].iloc[-1]:.2f}")
        if abs(df['ROC'].iloc[-1]) > 40:
            logger.warning(f"[Indicators] Unusually high ROC for {symbol}: {df['ROC'].iloc[-1]:.2f}%")

        # Handle NaN values
        indicator_cols = ['RSI', 'MACD', 'MACD_signal', 'MACD_hist', 'ADX', 'EMA20', 'EMA50', 'ATR', 'ROC']
        df[indicator_cols] = df[indicator_cols].replace([np.inf, -np.inf], np.nan)
        df[indicator_cols] = df[indicator_cols].ffill().bfill()
        if df[indicator_cols].isna().any().any():
            logger.warning(f"[Indicators] NaN values persist in indicators for {symbol}: {df[indicator_cols].isna().sum().to_dict()}")

        # Store metrics
        if not df.empty:
            metrics = {
                "symbol": symbol,
                "timestamp": int(df.index[-1].timestamp() * 1000),
                "rsi": float(df["RSI"].iloc[-1]),
                "macd": float(df["MACD"].iloc[-1]),
                "adx": float(df["ADX"].iloc[-1]),
                "ema20": float(df["EMA20"].iloc[-1]),
                "ema50": float(df["EMA50"].iloc[-1]),
                "atr": float(df["ATR"].iloc[-1]),
                "roc": float(df["ROC"].iloc[-1])
            }
            insert_metrics(symbol, metrics)
            logger.info(f"[Metrics Stored] {metrics}")

            # Display table
            #table = Table(title=f"Technical Indicators for {symbol}")
            #table.add_column("Indicator", style="cyan")
            #table.add_column("Value", style="magenta")
            #for key, value in metrics.items():
                #if key not in ["symbol", "timestamp"]:
                    #table.add_row(key.upper(), f"{value:.2f}")
            #console.log(table)

        logger.info(f"Indicators calculated for {symbol}")
        return df

    except Exception as e:
        logger.error(f"Error in calculate_indicators for {symbol}: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return df