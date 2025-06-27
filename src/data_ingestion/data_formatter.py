import pandas as pd
import json
import logging

logger = logging.getLogger(__name__)

def format_candle(candle_json):
    try:
        logger.debug(f"[Formatter] Raw input JSON: {candle_json}")
        candle = json.loads(candle_json)
        required_keys = ["timestamp", "open", "high", "low", "close", "volume"]
        if not all(key in candle for key in required_keys):
            logger.error(f"[Formatter] Missing keys in candle: {candle.keys()}")
            return pd.DataFrame()
        return pd.DataFrame([{
            "timestamp": pd.to_datetime(candle["timestamp"], unit="ms"),
            "open": float(candle["open"]),
            "high": float(candle["high"]),
            "low": float(candle["low"]),
            "close": float(candle["close"]),
            "volume": float(candle["volume"])
        }])
    except Exception as e:
        logger.error(f"[Formatter] Data formatting error: {str(e)}, Input: {candle_json}")
        return pd.DataFrame()

