# Signal Generator Module - Enhanced with Performance Tracking
import logging
import numpy as np
import pandas as pd
import talib
import time
from datetime import datetime, timedelta
from src.database.db_handler import insert_signal, insert_training_data, get_future_prices, get_training_data_count
from src.monitoring.alerting import send_telegram_alert

logger = logging.getLogger(__name__)

def prepare_lstm_input(df):
    required_cols = ['close', 'volume', 'RSI', 'MACD', 'ADX']
    data = df[required_cols].values[-100:].astype(float)
    if np.isnan(data).any():
        logger.warning("[Debug] NaN values detected in LSTM input, filling with forward fill")
        df_filled = df[required_cols].ffill().values[-100:]
        data = df_filled.astype(float)
    logger.info(f"[Debug] LSTM input shape: {data.shape}, contains nan: {np.isnan(data).any()}")
    return data.reshape(1, 100, len(required_cols))

def calculate_dynamic_thresholds(adx, strategy="trend"):
    base_up, base_down = {
        "trend": (0.55, 0.45),
        "scalp": (0.6, 0.4),
        "range": (0.52, 0.48)
    }.get(strategy, (0.5, 0.5))
    adj = (adx / 100) * (0.25 if strategy == "trend" else 0.2 if strategy == "scalp" else 0.1)
    up = max(base_up - adj, base_down + 0.1)
    down = min(base_down + adj, up - 0.1)
    return round(up, 3), round(down, 3)

def select_strategy_mode(adx, rsi, atr):
    if adx > 30:
        return "trend"
    elif adx < 15 and 40 < rsi < 60 and atr < 30:
        return "range"
    return "scalp"  # Default to scalp if conditions

def calculate_market_direction(symbol, signal_timestamp):
    """Calculate actual market movement after a trade signal"""
    try:
        future_df = get_future_prices(symbol, signal_timestamp, candle_count=5)
        if len(future_df) < 5:
            return None, None
        start_price = future_df.iloc[0]['close']
        end_price = future_df.iloc[-1]['close']
        price_change = end_price - start_price
        price_change_pct = (price_change / start_price) * 100
        direction = 1 if price_change > 0 else 0
        return direction, price_change_pct
    except Exception as e:
        logger.error(f"[Performance Tracking] Error calculating market direction: {e}")
        return None, None

def should_retrain_model():
    """Check if model should be retrained based on collected data"""
    if datetime.now().weekday() == 0:
        return True
    new_data_count = get_training_data_count(since_last_train=True)
    if new_data_count >= 100:
        return True
    return False

def check_signal(df, model, current_position, last_order_details, symbol, last_action_sent=None):
    if not hasattr(check_signal, 'last_df') or check_signal.last_df is not df:
        # Recalculate indicators if the DataFrame reference has changed
        check_signal.last_df = df
    # Use pre-calculated indicators

    logger.info(f"[Debug] Using model with last_train_time: {getattr(model, 'last_train_time', 'N/A')} for {symbol}")
    if len(df) < 100:
        return "None", None

    # --- Indicators ---
    rsi = df['RSI'].iloc[-1]
    macd = df['MACD'].iloc[-1]
    signal_line = df['MACD_signal'].iloc[-1]
    adx = df['ADX'].iloc[-1]
    ema20 = df['EMA20'].iloc[-1]
    ema50 = df['EMA50'].iloc[-1]
    atr = df['ATR'].iloc[-1]
    close = df['close'].iloc[-1]
    roc = talib.ROC(df['close'], timeperiod=5).iloc[-1] * 100

    strategy_mode = select_strategy_mode(adx, rsi, atr)
    logger.info(f"[Strategy] Switched to {strategy_mode}, ADX: {adx:.2f}, RSI: {rsi:.2f}, ATR: {atr:.2f}, Roc: {atr:.2f} for {symbol}")


    lstm_input = prepare_lstm_input(df)
    try:
        prediction = model.predict(lstm_input, verbose=0)[0][0]
    except Exception as e:
        logger.error(f"[Prediction Error] {e} for {symbol}")
        return

    dynamic_up, dynamic_down = calculate_dynamic_thresholds(adx, strategy_mode)
    trend_up = ema20 > ema50
    trend_down = ema20 < ema50
    macd_bullish = macd > signal_line
    rsi_strong = (trend_up and rsi > 55) or (trend_down and rsi < 40) or (abs(rsi - 50) > 20)
    rolling_high = df['high'].rolling(window=20).max()
    rolling_low = df['low'].rolling(window=20).min()
    breakout_up = close > (rolling_high.iloc[-1] - 0.3 * atr) if len(df) >= 20 else False
    breakout_down = close < (rolling_low.iloc[-1] + 0.3 * atr) if len(df) >= 20 else False
    bullish_divergence = (df['close'].iloc[-1] < df['close'].iloc[-3] and df['RSI'].iloc[-1] > df['RSI'].iloc[-3])
    bearish_divergence = (df['close'].iloc[-1] > df['close'].iloc[-3] and df['RSI'].iloc[-1] < df['RSI'].iloc[-3])

    action = "None"
    new_position = None
    signal_timestamp = int(time.time() * 1000)

    # --- Signal logic (same as previous, can be customized) ---
    if strategy_mode == "scalp" and rsi_strong:
        if prediction > dynamic_up + 0.1 and roc > 1.0:  # Higher confidence
            action = "Sell"
            new_position = "short"
        elif prediction < dynamic_down - 0.1 and roc < -1.0:
            action = "buy"
            new_position = "long"
    elif strategy_mode == "trend":
        if (trend_up and macd_bullish and rsi_strong and prediction > dynamic_up and breakout_up and roc > 0.5):
            action = "sell"
            new_position = "shrort"
        elif (trend_down and not macd_bullish and rsi_strong and prediction < dynamic_down and breakout_down and roc < -0.5):
            action = "buy"
            new_position = "long"
    elif strategy_mode == "range":
        range_high = rolling_high.iloc[-1]
        range_low = rolling_low.iloc[-1]
        if close <= (range_low + 0.2 * atr) and prediction > dynamic_up and roc > 0.5:
            action = "buy"
            new_position = "long"
        elif close >= (range_high - 0.2 * atr) and prediction < dynamic_down and roc < -0.5:
            action = "sell"
            new_position = "short"

    if bullish_divergence and prediction > 0.6 and roc > 1.5:
        action = "buy"
        new_position = "long"
    elif bearish_divergence and prediction < 0.4 and roc < -1.5:
        action = "sell"
        new_position = "short"

    if current_position == "long" and (trend_down or not macd_bullish or roc < -1.0):
        action = "close_buy"
        new_position = None
    elif current_position == "short" and (trend_up or macd_bullish or roc > 1.0):
        action = "close_sell"
        new_position = None

    # Anti-redundancy
    if action == last_action_sent:
        logger.info(f"[Anti-Repeat] Signal {action} ignored for {symbol} as it was sent previously.")
        return "None", current_position

    # Store signal and training data for model improvement
    if action in ("buy", "sell"):
        try:
            training_record = {
                "symbol": symbol,
                "timestamp": signal_timestamp,
                "prediction": float(prediction),
                "action": action,
                "price": float(close),
                "indicators": {
                    "rsi": round(rsi, 2),
                    "macd": round(macd, 4),
                    "adx": round(adx, 2),
                    "roc": round(roc, 2),
                    "ema20": round(ema20, 4),
                    "ema50": round(ema50, 4),
                    "atr": round(atr, 4),
                    "strategy_mode": strategy_mode
                },
                "market_context": {
                    "trend_up": trend_up,
                    "trend_down": trend_down,
                    "macd_bullish": macd_bullish,
                    "breakout_up": breakout_up,
                    "breakout_down": breakout_down
                }
            }
            training_record["check_timestamp"] = int((datetime.now() + timedelta(minutes=5)).timestamp() * 1000)
            insert_training_data(training_record)
            logger.info(f"[Performance Tracking] Stored training data for {symbol} signal")
            check_time = datetime.now() + timedelta(minutes=5)
            logger.info(f"[Performance Tracking] Will verify market direction at {check_time.strftime('%H:%M')}")
        except Exception as e:
            logger.error(f"[Performance Tracking] Error storing training data: {e}")

    if action != "None":
        signal_details = {
            "symbol": symbol,
            "signal_type": action,
            "price": float(close),
            "timestamp": signal_timestamp,
            "quantity": 0.0,
            "indicators": {
                "rsi": round(rsi, 2),
                "macd": round(macd, 4),
                "adx": round(adx, 2),
                "roc": round(roc, 2),
                "strategy": strategy_mode
            }
        }
        insert_signal(signal_details)
        logger.info(f"[Signal Stored] {action} at {close} for {symbol}")
    else:
        logger.info(f"[No Signal] Conditions not met for {symbol}")

    return action, new_position
