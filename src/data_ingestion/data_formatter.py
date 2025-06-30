import pandas as pd
import logging

logger = logging.getLogger(__name__)

def format_candle(candle, symbol):
    """
    Formate une bougie (candle) brute en DataFrame pandas standardisée pour l'insertion en base.
    """
    try:
        # Vérifier si 'T' ou 'timestamp' est présent pour le timestamp
        timestamp_key = 'T' if 'T' in candle else 'timestamp'
        candle_data = {
            'timestamp': pd.to_datetime(candle[timestamp_key], unit='ms'),
            'open': float(candle['o' if 'o' in candle else 'open']),
            'high': float(candle['h' if 'h' in candle else 'high']),
            'low': float(candle['l' if 'l' in candle else 'low']),
            'close': float(candle['c' if 'c' in candle else 'close']),
            'volume': float(candle['v' if 'v' in candle else 'volume'])
        }
        df = pd.DataFrame([candle_data])
        logger.debug(f"[format_candle] Formatted DataFrame for {symbol}: {df.to_dict()}")
        return df
    except Exception as e:
        logger.error(f"[format_candle] Error formatting candle for {symbol}: {e} | data: {candle}", exc_info=True)
        return pd.DataFrame()