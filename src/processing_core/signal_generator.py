import pandas as pd
import numpy as np
from src.processing_core.indicators import calculate_indicators
from src.processing_core.lstm_model import SEQ_LEN

def detect_breakout(df):
    if len(df) < 21:
        return None
    try:
        window_high = df["high"].iloc[-21:-1].max()
        window_low = df["low"].iloc[-21:-1].min()
        close = df["close"].iloc[-1]
        if close > window_high:
            return "breakout_up"
        elif close < window_low:
            return "breakout_down"
        return None
    except Exception as e:
        print(f"Breakout detection error: {e}")
        return None

def check_signal(df, model, position_type=None):
    if len(df) < 51 or model is None:
        return None, None
    try:
        df = calculate_indicators(df)
        last = df.iloc[-1]
        print(f"Indicator values - RSI: {last['RSI']}, MACD: {last['MACD']}, Signal: {last['MACD_signal']}, "
              f"ADX: {last['ADX']}, EMA20: {last['EMA20']}, EMA50: {last['EMA50']}")
        
        # Check for exit conditions if a position is open
        action = None
        new_position = position_type
        if position_type == "buy":
            if last["RSI"] > 70 or last["MACD"] < last["MACD_signal"]:
                print("Exit condition met: Closing BUY position")
                return "close_buy", None  # Close position, no new position
        elif position_type == "sell":
            if last["RSI"] < 30 or last["MACD"] > last["MACD_signal"]:
                print("Exit condition met: Closing SELL position")
                return "close_sell", None  # Close position, no new position

        # Check for entry conditions if no position or after closing
        trend_up = (
            last["EMA20"] > last["EMA50"]
            and last["MACD"] > last["MACD_signal"]
            and last["ADX"] > 20
            and last["RSI"] > 60
        )
        trend_down = (
            last["EMA20"] < last["EMA50"]
            and last["MACD"] < last["MACD_signal"]
            and last["ADX"] > 20
            and last["RSI"] < 40
        )
        breakout = detect_breakout(df)
        print(f"Breakout: {breakout}, Trend Up: {trend_up}, Trend Down: {trend_down}")
        seq_data = df[["close", "volume", "RSI", "MACD", "ADX"]].dropna().values
        if len(seq_data) < SEQ_LEN:
            print(f"Insufficient sequence data: {len(seq_data)} < {SEQ_LEN}")
            return None, position_type
        lstm_input = np.expand_dims(seq_data[-SEQ_LEN:], axis=0)
        pred = model.predict(lstm_input, verbose=0)[0][0]
        print(f"LSTM Prediction: {pred}")
        if trend_up and pred > 0.6:
            action = "buy"
            new_position = "buy"
        elif trend_down and pred < 0.4:
            action = "sell"
            new_position = "sell"
        return action, new_position
    except Exception as e:
        print(f"Signal generation error: {e}")
        return None, position_type