import pandas as pd
import json
import logging

logger = logging.getLogger(__name__)

def format_candle(candle_json):
    try:
        candle = json.loads(candle_json)
        required_keys = ["timestamp", "open", "high", "low", "close", "volume"]
        if not all(key in candle for key in required_keys):
            logger.error(f"Missing keys: {candle.keys()}")
            return pd.DataFrame()
        return pd.DataFrame([{
            "timestamp": pd.to_datetime(candle["timestamp"], unit="ms"),
            "open": candle["open"],
            "high": candle["high"],
            "low": candle["low"],
            "close": candle["close"],
            "volume": candle["volume"]
        }])
    except Exception as e:
        logger.error(f"Data formatting error: {e}")
        return pd.DataFrame()
