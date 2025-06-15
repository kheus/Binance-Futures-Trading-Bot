# Signal Generator Module
import logging
from logging import config
import numpy as np
import pandas as pd
import talib
import time
from src.database.db_handler import insert_signal
from src.monitoring.alerting import send_telegram_alert

logger = logging.getLogger(__name__)

def prepare_lstm_input(df):
    required_cols = ['close', 'volume', 'RSI', 'MACD', 'ADX']
    data = df[required_cols].values[-100:].astype(float)  # Aligne sur SEQ_LEN = 100
    if np.isnan(data).any():
        logger.warning("[Debug] NaN values detected in LSTM input, filling with 0")
        data = np.nan_to_num(data, nan=0.0)
    logger.info(f"[Debug] LSTM input shape: {data.shape}, contains nan: {np.isnan(data).any()}")
    logger.info(f"[Debug] LSTM Input last row: {df[required_cols].iloc[-1].to_dict()}")
    return data.reshape(1, 100, len(required_cols))

def calculate_dynamic_thresholds(adx, strategy="trend"):
    if strategy == "trend":
        up = 0.55 - (adx / 100) * 0.25
        down = 0.45 + (adx / 100) * 0.25
    elif strategy == "scalp":
        up = 0.6 - (adx / 100) * 0.2
        down = 0.4 + (adx / 100) * 0.2
    elif strategy == "range":
        up = 0.52 - (adx / 100) * 0.1
        down = 0.48 + (adx / 100) * 0.1
    else:
        up, down = 0.5, 0.5
    return round(up, 3), round(down, 3)

def select_strategy_mode(adx, rsi, atr):
    if adx > 30:  # Forte tendance
        return "trend"
    elif adx < 15 and 40 < rsi < 60 and atr < 30:  # Marche plat avec faible volatilité
        return "range"
    else:  # Volatilité modérée ou conditions mixtes
        return "scalp"

def check_signal(df, model, current_position, last_order_details, symbol, last_action_sent=None):
    logger.info(f"[Debug] Using model with last_train_time: {getattr(model, 'last_train_time', 'N/A')} for {symbol}")
    if len(df) < 100:
        return "None", None

    # Indicateurs
    rsi = df['RSI'].iloc[-1]
    macd = df['MACD'].iloc[-1]
    signal = df['MACD_signal'].iloc[-1]
    adx = df['ADX'].iloc[-1]
    ema20 = df['EMA20'].iloc[-1]
    ema50 = df['EMA50'].iloc[-1]
    atr = df['ATR'].iloc[-1]
    close = df['close'].iloc[-1]

    strategy_mode = select_strategy_mode(adx, rsi, atr)
    logger.info(f"[Strategy] Switched to {strategy_mode}, ADX: {adx:.2f}, RSI: {rsi:.2f}, ATR: {atr:.2f} for {symbol}")

    lstm_input = prepare_lstm_input(df)
    try:
        prediction = model.predict(lstm_input, verbose=0)[0][0]
        logger.info(f"[Prediction Output] {prediction} for {symbol}")
    except Exception as e:
        logger.error(f"[Prediction Error] {e} for {symbol}")
        prediction = 0.5

    dynamic_up, dynamic_down = calculate_dynamic_thresholds(adx, strategy_mode)
    trend_up = ema20 > ema50
    trend_down = ema20 < ema50
    macd_bullish = macd > signal
    rsi_strong = (rsi > 50 and trend_up) or (rsi < 50 and trend_down) or (rsi > 55 or rsi < 40)
    breakout_up = close > df['high'].rolling(window=20).max().iloc[-1] if len(df) >= 20 else False
    breakout_down = close < df['low'].rolling(window=20).min().iloc[-1] if len(df) >= 20 else False

    action = "None"
    new_position = None
    # Remove ATR check for ETHUSDT, SOLUSDT, XRPUSDT
    if symbol not in ["ETHUSDT", "SOLUSDT", "XRPUSDT"]:
        if atr < 20:
            logger.warning(f"[Volatility] ATR too low: {atr} < 20, No trade for {symbol}")
            return "None", current_position

    if strategy_mode == "scalp":
        if rsi_strong and prediction > dynamic_up:
            action = "buy"
            new_position = "long"
        elif rsi_strong and prediction < dynamic_down:
            action = "sell"
            new_position = "short"
    elif strategy_mode == "trend":
        if trend_up and macd_bullish and rsi_strong and prediction > dynamic_up and breakout_up:
            action = "buy"
            new_position = "long"
        elif trend_down and not macd_bullish and rsi_strong and prediction < dynamic_down and breakout_down:
            action = "sell"
            new_position = "short"
    elif strategy_mode == "range":
        range_high = df['high'].rolling(20).max().iloc[-1]
        range_low = df['low'].rolling(20).min().iloc[-1]
        if close <= range_low and prediction > dynamic_up:
            action = "buy"
            new_position = "long"
        elif close >= range_high and prediction < dynamic_down:
            action = "sell"
            new_position = "short"

    # Clôtures intelligentes
    if current_position == "long" and (trend_down or not macd_bullish):
        action = "close_buy"
        new_position = None
    elif current_position == "short" and (trend_up or macd_bullish):
        action = "close_sell"
        new_position = None

    # 🔁 Filtrage anti-redondance
    if action == last_action_sent:
        logger.info(f"[Anti-Repeat] Signal {action} ignored for {symbol} as it was sent previously.")
        return "None", current_position

    # Stockage du signal si nouveau
    if action != "None":
        signal_details = {
            "symbol": symbol,
            "signal_type": action,
            "price": float(close),
            "timestamp": int(time.time() * 1000),
            "quantity": 0.0
        }
        insert_signal(signal_details)
        logger.info(f"[Signal Stored] {signal_details} for {symbol}")
    else:
        logger.info(f"[No DB Insert] Action is 'None' for {symbol}, skipping database insert")

    return action, new_position