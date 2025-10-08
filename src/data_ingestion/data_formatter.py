import pandas as pd
import logging

logger = logging.getLogger(__name__)

def format_candle(candle, symbol):
    """
    Formate une bougie (candle) brute provenant de Binance (liste ou dict)
    en DataFrame standardisée pour insertion en base.
    """
    try:
        # ✅ 1. Si c'est une liste -> convertir en dictionnaire Binance standard
        if isinstance(candle, list):
            if len(candle) < 6:
                logger.error(f"[format_candle] Candle list too short for {symbol}: {candle}")
                return pd.DataFrame()

            candle = {
                "T": int(candle[0]),      # Open time
                "o": float(candle[1]),    # Open
                "h": float(candle[2]),    # High
                "l": float(candle[3]),    # Low
                "c": float(candle[4]),    # Close
                "v": float(candle[5])     # Volume
            }

        # ✅ 2. Vérification minimale du format dict
        elif not isinstance(candle, dict):
            logger.error(f"[format_candle] Unexpected candle type for {symbol}: {type(candle)}")
            return pd.DataFrame()

        # ✅ 3. Identifier les clés disponibles
        required_keys = ['T', 'o', 'h', 'l', 'c', 'v']
        alt_required_keys = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
        use_alt_keys = all(k in candle for k in alt_required_keys)

        if not all(k in candle for k in required_keys) and not use_alt_keys:
            logger.error(f"[format_candle] Missing required keys for {symbol}: {candle}")
            return pd.DataFrame()

        # ✅ 4. Sélection des bons noms de clé
        timestamp_key = 'timestamp' if use_alt_keys else 'T'
        open_key = 'open' if use_alt_keys else 'o'
        high_key = 'high' if use_alt_keys else 'h'
        low_key = 'low' if use_alt_keys else 'l'
        close_key = 'close' if use_alt_keys else 'c'
        volume_key = 'volume' if use_alt_keys else 'v'

        # ✅ 5. Conversion des valeurs
        timestamp = pd.to_datetime(candle[timestamp_key], unit='ms')
        candle_data = {
            'timestamp': timestamp,
            'open': float(candle[open_key]),
            'high': float(candle[high_key]),
            'low': float(candle[low_key]),
            'close': float(candle[close_key]),
            'volume': float(candle[volume_key])
        }

        df = pd.DataFrame([candle_data]).set_index('timestamp')
        logger.debug(f"[format_candle] Formatted DataFrame for {symbol}: {df.reset_index().to_dict(orient='records')}")
        return df

    except Exception as e:
        logger.error(f"[format_candle] Error formatting candle for {symbol}: {e} | data: {candle}", exc_info=True)
        return pd.DataFrame()
