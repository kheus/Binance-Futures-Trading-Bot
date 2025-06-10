import logging
import numpy as np
import pandas as pd
import talib
from src.monitoring.alerting import send_telegram_alert

logger = logging.getLogger(__name__)

def prepare_lstm_input(df):
    required_cols = ['close', 'volume', 'RSI', 'MACD', 'ADX']
    data = df[required_cols].values[-100:].astype(float)  # Aligné sur SEQ_LEN = 100
    logger.info(f"[Debug] LSTM input shape: {data.shape}, contains nan: {np.isnan(data).any()}")
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
    elif adx < 15 and rsi > 40 and rsi < 60 and atr < 30:  # Marché plat avec faible volatilité
        return "range"
    else:  # Volatilité modérée ou conditions mixtes
        return "scalp"

def check_signal(df, model, current_position, last_order_details):
    logger.info(f"[Debug] Using model with last_train_time: {getattr(model, 'last_train_time', 'N/A')}")
    if len(df) < 100:  # Aligné sur SEQ_LEN
        return "None", None

    # Calculer les indicateurs
    rsi = df['RSI'].iloc[-1]
    macd = df['MACD'].iloc[-1]
    signal = df['MACD_signal'].iloc[-1]
    adx = df['ADX'].iloc[-1]
    ema20 = df['EMA20'].iloc[-1]
    ema50 = df['EMA50'].iloc[-1]
    atr = df['ATR'].iloc[-1]
    close = df['close'].iloc[-1]

    # Déterminer la stratégie automatiquement
    strategy_mode = select_strategy_mode(adx, rsi, atr)
    logger.info(f"[Strategy] Switched to {strategy_mode}, ADX: {adx:.2f}, RSI: {rsi:.2f}, ATR: {atr:.2f}")

    # Préparer l'entrée LSTM
    lstm_input = prepare_lstm_input(df)
    try:
        prediction = model.predict(lstm_input, verbose=0)[0][0]
        logger.info(f"[Prediction Output] {prediction}")
    except Exception as e:
        logger.error(f"[Prediction Error] {e}")
        prediction = 0.5  # Valeur par défaut

    # Calculer les seuils dynamiques
    dynamic_up, dynamic_down = calculate_dynamic_thresholds(adx, strategy_mode)
    logger.info(f"[Dynamic Thresholds] ADX: {adx:.2f}, Up: {dynamic_up:.3f}, Down: {dynamic_down:.3f}")

    # Calculs de base
    trend_up = ema20 > ema50
    trend_down = ema20 < ema50
    macd_bullish = macd > signal
    # Ajustement contextuel du RSI
    rsi_strong = (rsi > 50 and trend_up) or (rsi < 50 and trend_down) or (rsi > 55 or rsi < 40)
    breakout_up = close > df['high'].rolling(window=20).max().iloc[-1] if len(df) >= 20 else False
    breakout_down = close < df['low'].rolling(window=20).min().iloc[-1] if len(df) >= 20 else False
    logger.info(f"[Indicators] RSI: {rsi}, MACD: {macd}, Signal: {signal}, ADX: {adx}, EMA20: {ema20}, EMA50: {ema50}, ATR: {atr}")
    logger.info(f"[Conditions] MACD Bullish: {macd_bullish}, RSI Strong: {rsi_strong}, ADX Strong: {adx > 25}")
    logger.info(f"[Debug] Breakout Up Check: close={close}, max_20={df['high'].rolling(window=20).max().iloc[-1]}")
    logger.info(f"[Trend Up] {trend_up}, Breakout: {breakout_up}")
    logger.info(f"[Trend Down] {trend_down}, Breakout: {breakout_down}")

    action = "None"
    new_position = None
    if atr < 20:
        logger.warning(f"[Volatility] ATR too low: {atr} < 20, No trade")
    elif strategy_mode == "scalp":
        logger.info(f"[Debug] Scalp Decision: rsi_strong={rsi_strong}, pred={prediction}, dynamic_up={dynamic_up}, dynamic_down={dynamic_down}")
        if rsi_strong and prediction > dynamic_up:
            action = "buy"
            new_position = "long"
        elif rsi_strong and prediction < dynamic_down:
            action = "sell"
            new_position = "short"
    elif strategy_mode == "trend":
        if trend_up and macd_bullish and rsi_strong and prediction > dynamic_up and (breakout_up or True):  # Breakout optionnel
            action = "buy"
            new_position = "long"
        elif trend_down and not macd_bullish and rsi_strong and prediction < dynamic_down and breakout_down:
            action = "sell"
            new_position = "short"
    elif strategy_mode == "range":
        range_high = df['high'].rolling(20).max().iloc[-1]
        range_low = df['low'].rolling(window=20).min().iloc[-1]
        if close <= range_low and prediction > dynamic_up:
            action = "buy"
            new_position = "long"
        elif close >= range_high and prediction < dynamic_down:
            action = "sell"
            new_position = "short"

    # Gestion des fermetures
    if current_position == "long" and (trend_down or not macd_bullish):
        action = "close_buy"
        new_position = None
    elif current_position == "short" and (trend_up or macd_bullish):
        action = "close_sell"
        new_position = None

    logger.info(f"[Decision] Trend Up: {trend_up}, MACD Bullish: {macd_bullish}, RSI Strong: {rsi_strong}, Pred > Dynamic Up: {prediction > dynamic_up}, Breakout: {breakout_up}")
    if action == "None":
        logger.info("[No Entry] No condition met for new position")
    return action, new_position