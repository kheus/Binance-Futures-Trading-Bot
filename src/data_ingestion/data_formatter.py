# src/data_ingestion/data_formatter.py
import pandas as pd
import logging

logger = logging.getLogger(__name__)

def format_candle(candle, symbol):
    """
    Formate une bougie (candle) brute en DataFrame pandas standardisée pour l'insertion en base.
    """
    try:
        if not isinstance(candle, dict):
            logger.error(f"[format_candle] Candle is not a dictionary for {symbol}: {candle}")
            return pd.DataFrame()

        required_keys = ['T', 'o', 'h', 'l', 'c', 'v']
        alt_required_keys = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
        use_alt_keys = all(key in candle for key in alt_required_keys)
        if not all(key in candle for key in required_keys) and not use_alt_keys:
            logger.error(f"[format_candle] Missing required keys for {symbol}: {candle}")
            return pd.DataFrame()

        timestamp_key = 'timestamp' if use_alt_keys else 'T'
        open_key = 'open' if use_alt_keys else 'o'
        high_key = 'high' if use_alt_keys else 'h'
        low_key = 'low' if use_alt_keys else 'l'
        close_key = 'close' if use_alt_keys else 'c'
        volume_key = 'volume' if use_alt_keys else 'v'

        logger.debug(f"[format_candle] Using {'alternative' if use_alt_keys else 'standard'} key format for {symbol}")

        try:
            timestamp = pd.to_datetime(candle[timestamp_key], unit='ms')
        except (ValueError, TypeError) as e:
            logger.error(f"[format_candle] Invalid timestamp format for {symbol}: {candle[timestamp_key]}, error: {e}")
            return pd.DataFrame()

        for key in [open_key, high_key, low_key, close_key, volume_key]:
            try:
                float(candle[key])
            except (ValueError, TypeError) as e:
                logger.error(f"[format_candle] Invalid numeric value for {key} in {symbol}: {candle[key]}, error: {e}")
                return pd.DataFrame()

        candle_data = {
            'timestamp': timestamp,
            'open': float(candle[open_key]),
            'high': float(candle[high_key]),
            'low': float(candle[low_key]),
            'close': float(candle[close_key]),
            'volume': float(candle[volume_key])
        }
        df = pd.DataFrame([candle_data])
        df.set_index('timestamp', inplace=True)
        logger.debug(f"[format_candle] Formatted DataFrame for {symbol}: {df.reset_index().to_dict(orient='records')}")
        return df
    except Exception as e:
        logger.error(f"[format_candle] Error formatting candle for {symbol}: {e} | data: {candle}", exc_info=True)
        return pd.DataFrame()