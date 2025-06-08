import pandas as pd
import json

def format_candle(candle_json):
    try:
        candle = json.loads(candle_json)
        return pd.DataFrame([{
            "timestamp": pd.to_datetime(candle["timestamp"], unit="ms"),
            "open": candle["open"],
            "high": candle["high"],
            "low": candle["low"],
            "close": candle["close"],
            "volume": candle["volume"]
        }])
    except Exception as e:
        print(f"Data formatting error: {e}")
        return pd.DataFrame()
