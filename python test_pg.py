# File: src/processing_core/signal_generator.py

import logging
import numpy as np
import pandas as pd
import talib
from datetime import datetime, timedelta

from src.database.db_handler import insert_signal, insert_training_data, get_future_prices, get_training_data_count
from src.monitoring.alerting import send_telegram_alert
from rich.console import Console
from rich.table import Table

logger = logging.getLogger(__name__)
console = Console()

REQUIRED_COLS = ['close', 'volume', 'RSI', 'MACD', 'MACD_signal', 'MACD_hist', 'ADX', 'ROC']

def prepare_lstm_input(df, window=100):
    if not all(col in df.columns for col in REQUIRED_COLS):
        logger.error(f"[Data Error] Missing columns: {set(REQUIRED_COLS) - set(df.columns)}")
        return None
    data = df[REQUIRED_COLS].fillna(method='ffill').values[-window:].astype(float)
    return data.reshape(1, window, len(REQUIRED_COLS))

def calculate_dynamic_thresholds(adx, strategy):
    base_up, base_down = {
        "trend": (0.48, 0.43),
        "scalp": (0.55, 0.45),
        "range": (0.50, 0.50)
    }.get(strategy, (0.50, 0.50))
    adj = adx / 2000
    up = max(base_up - adj, base_down + 0.03)
    down = min(base_down + adj, up - 0.03)
    return round(up, 3), round(down, 3)

def select_strategy_mode(adx, rsi, atr):
    if adx > 30:
        return "trend"
    elif adx < 15 and 40 < rsi < 60 and atr < 30:
        return "range"
    return "scalp"

def log_indicator_summary(symbol, indicators, confidence_factors):
    table = Table(title=f"Indicator Summary for {symbol}")
    table.add_column("Indicator", style="cyan")
    table.add_column("Value", style="magenta")
    table.add_column("Interpretation", style="green")

    table.add_row("RSI", f"{indicators['rsi']:.2f}", "Overbought" if indicators['rsi'] > 70 else "Oversold" if indicators['rsi'] < 30 else "Neutral")
    table.add_row("MACD", f"{indicators['macd']:.4f}", "Bullish" if indicators['macd'] > 0.5 else "Bearish" if indicators['macd'] < -0.5 else "Neutral")
    table.add_row("ADX", f"{indicators['adx']:.2f}", "Strong" if indicators['adx'] >= 50 else "Moderate" if indicators['adx'] >= 25 else "Weak")
    table.add_row("EMA", f"{indicators['ema20']:.2f}/{indicators['ema50']:.2f}", "Bullish" if indicators['ema20'] > indicators['ema50'] else "Bearish")
    table.add_row("ROC", f"{indicators['roc']:.2f}%", "Momentum")
    table.add_row("Confidence", ", ".join(confidence_factors) if confidence_factors else "None", "")
    console.log(table)

def check_signal(df, model, current_position, last_order_details, symbol, last_action_sent=None, config=None):
    if len(df) < 100:
        return "hold", current_position, 0.0, []

    # Indicators
    rsi = df['RSI'].iloc[-1]
    macd = df['MACD'].iloc[-1]
    signal_line = df['MACD_signal'].iloc[-1]
    macd_hist = df['MACD_hist'].iloc[-1]
    adx = df['ADX'].iloc[-1]
    ema20 = df['EMA20'].iloc[-1]
    ema50 = df['EMA50'].iloc[-1]
    atr = df['ATR'].iloc[-1]
    close = df['close'].iloc[-1]
    roc = talib.ROC(df['close'], timeperiod=5).iloc[-1] * 100

    strategy = select_strategy_mode(adx, rsi, atr)
    dynamic_up, dynamic_down = calculate_dynamic_thresholds(adx, strategy)
    
    lstm_input = prepare_lstm_input(df)
    if lstm_input is None:
        return "hold", current_position, 0.0, []

    try:
        prediction = model.predict(lstm_input, verbose=0)[0][0]
    except Exception as e:
        logger.error(f"[LSTM Error] {symbol}: {e}")
        return "hold", current_position, 0.0, []

    # Logic
    trend_up = ema20 > ema50
    trend_down = ema20 < ema50
    macd_bullish = macd > signal_line
    rsi_strong = rsi > 60 or rsi < 40

    rolling_high = df['high'].rolling(window=20).max().iloc[-1]
    rolling_low = df['low'].rolling(window=20).min().iloc[-1]
    breakout_up = close > (rolling_high - 0.1 * atr)
    breakout_down = close < (rolling_low + 0.1 * atr)
    
    bullish_div = df['close'].iloc[-1] < df['close'].iloc[-3] and rsi > df['RSI'].iloc[-3]
    bearish_div = df['close'].iloc[-1] > df['close'].iloc[-3] and rsi < df['RSI'].iloc[-3]

    # Confidence factors
    confidence_factors = []
    if prediction > 0.55 or prediction < 0.45:
        confidence_factors.append("LSTM strong")
    if trend_up and macd_bullish or trend_down and not macd_bullish:
        confidence_factors.append("MACD aligned")
    if rsi_strong:
        confidence_factors.append("RSI strong")
    if abs(roc) > 0.3:
        confidence_factors.append("Momentum")
    if breakout_up or breakout_down:
        confidence_factors.append("Breakout")

    indicators = {
        "rsi": rsi, "macd": macd, "macd_hist": macd_hist, "adx": adx, "roc": roc,
        "ema20": ema20, "ema50": ema50, "atr": atr, "strategy_mode": strategy
    }

    log_indicator_summary(symbol, indicators, confidence_factors)

    action = "hold"
    new_position = None
    timestamp = int(df.index[-1].timestamp() * 1000)
    capital = config['binance'].get('capital', 1000)
    leverage = config['binance'].get('leverage', 50)
    risk = 0.02 * capital
    quantity = min((capital * leverage) / close, risk / (atr * 2)) if close > 0 else 0.0

    if strategy == "scalp" and rsi_strong:
        if prediction > dynamic_up and roc > 0.3:
            action = "sell"; new_position = {"side": "short", "quantity": quantity, "price": close, "trade_id": str(timestamp)}
        elif prediction < dynamic_down and roc < -0.3:
            action = "buy"; new_position = {"side": "long", "quantity": quantity, "price": close, "trade_id": str(timestamp)}
    elif strategy == "trend":
        if trend_up and macd_bullish and prediction > dynamic_up:
            action = "sell"; new_position = {"side": "short", "quantity": quantity, "price": close, "trade_id": str(timestamp)}
        elif trend_down and not macd_bullish and prediction < dynamic_down:
            action = "buy"; new_position = {"side": "long", "quantity": quantity, "price": close, "trade_id": str(timestamp)}
    elif strategy == "range":
        if close <= rolling_low + 0.1 * atr and prediction > dynamic_up:
            action = "buy"; new_position = {"side": "long", "quantity": quantity, "price": close, "trade_id": str(timestamp)}
        elif close >= rolling_high - 0.1 * atr and prediction < dynamic_down:
            action = "sell"; new_position = {"side": "short", "quantity": quantity, "price": close, "trade_id": str(timestamp)}

    if bullish_div and prediction > 0.5:
        action = "buy"; new_position = {"side": "long", "quantity": quantity, "price": close, "trade_id": str(timestamp)}
    elif bearish_div and prediction < 0.5:
        action = "sell"; new_position = {"side": "short", "quantity": quantity, "price": close, "trade_id": str(timestamp)}

    if current_position == "long" and (trend_down or not macd_bullish or roc < -0.3):
        action, new_position = "close_buy", None
    elif current_position == "short" and (trend_up or macd_bullish or roc > 0.3):
        action, new_position = "close_sell", None

    if last_action_sent and isinstance(last_action_sent, tuple) and action == last_action_sent[0]:
        logger.info(f"[Anti-Repeat] Ignoring repeated signal: {action} for {symbol}")
        return "hold", current_position, 0.0, []

    if action != "hold":
        send_telegram_alert(f"[Signal] {symbol} → Action: {action.upper()} | Conf: {len(confidence_factors)}/6 | Pred: {prediction:.4f}")

    try:
        insert_training_data(symbol, timestamp, indicators, {
            "trend_up": trend_up, "trend_down": trend_down,
            "macd_bullish": macd_bullish, "breakout_up": breakout_up, "breakout_down": breakout_down
        }, prediction=prediction, action=action, price=close)
    except Exception as e:
        logger.error(f"[Training Error] {symbol}: {e}")

    if action in ["buy", "sell"]:
        return action, new_position, len(confidence_factors)/6, confidence_factors

    return action, None, len(confidence_factors)/6, confidence_factors
