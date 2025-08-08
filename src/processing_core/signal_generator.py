"""
Signal Generator Module - Core Trading Signal Generation

POTENTIAL AREAS FOR ENHANCEMENT:

1. ADVANCED MARKET REGIME DETECTION
   - TODO: Implement volatility clustering detection using GARCH models
   - TODO: Add market microstructure analysis (bid-ask spread, order book depth)
   - TODO: Incorporate economic calendar events and news sentiment
   - TODO: Use machine learning for automatic regime classification

2. ENHANCED STRATEGY SELECTION
   - TODO: Replace simple ADX/RSI thresholds with ML-based regime detection
   - TODO: Add momentum-based strategy for trending markets
   - TODO: Implement mean-reversion strategy with statistical tests
   - TODO: Dynamic strategy switching based on recent performance

3. IMPROVED CONFIDENCE SCORING
   - TODO: Implement adaptive confidence weights based on recent performance
   - TODO: Add cross-validation for confidence factor effectiveness
   - TODO: Include volume profile analysis in confidence calculation
   - TODO: Add correlation analysis between indicators to avoid redundancy

4. ADVANCED RISK MANAGEMENT
   - TODO: Implement Value at Risk (VaR) and Expected Shortfall calculations
   - TODO: Add portfolio-level risk metrics (correlation, beta)
   - TODO: Dynamic position sizing based on Kelly Criterion
   - TODO: Implement stop-loss and take-profit optimization

5. LSTM MODEL IMPROVEMENTS
   - TODO: Add attention mechanisms to LSTM architecture
   - TODO: Implement ensemble methods (multiple models voting)
   - TODO: Add feature engineering (technical patterns, market structure)
   - TODO: Implement online learning for model adaptation

6. PERFORMANCE OPTIMIZATION
   - TODO: Add signal performance attribution analysis
   - TODO: Implement A/B testing framework for strategy comparison
   - TODO: Add real-time model performance monitoring
   - TODO: Implement automatic model retraining triggers

7. ADDITIONAL TECHNICAL INDICATORS
   - TODO: Add Bollinger Bands for volatility-based signals
   - TODO: Implement Ichimoku Cloud for comprehensive trend analysis
   - TODO: Add Volume Weighted Average Price (VWAP) analysis
   - TODO: Include Fibonacci retracement levels

8. MARKET MICROSTRUCTURE
   - TODO: Add order flow analysis (large orders, institutional activity)
   - TODO: Implement tick-by-tick analysis for high-frequency signals
   - TODO: Add market depth analysis for liquidity assessment
   - TODO: Include time-of-day and day-of-week effects

9. SIGNAL FILTERING AND VALIDATION
   - TODO: Add Monte Carlo simulation for signal validation
   - TODO: Implement walk-forward analysis for robustness testing
   - TODO: Add correlation filters to avoid similar signals
   - TODO: Include market condition filters (low liquidity, high volatility)

10. EXECUTION OPTIMIZATION
    - TODO: Add slippage prediction models
    - TODO: Implement optimal execution algorithms (TWAP, VWAP)
    - TODO: Add latency monitoring and optimization
    - TODO: Include transaction cost analysis
"""

from datetime import datetime, timedelta
import logging
import json
import numpy as np
import pandas as pd
import talib
import time
from websocket import send
from src.database.db_handler import insert_signal, insert_training_data, get_future_prices, get_training_data_count
from src.monitoring.alerting import send_telegram_alert
from rich.console import Console
from rich.table import Table

logger = logging.getLogger(__name__)
console = Console()

def prepare_lstm_input(df):
    required_cols = ['close', 'volume', 'RSI', 'MACD', 'ADX']
    data = df[required_cols].values[-100:].astype(float)
    if np.isnan(data).any():
        logger.warning("[Debug] NaN values detected in LSTM input, filling with forward fill")
        df_filled = df[required_cols].ffill().values[-100:]
        data = np.nan_to_num(df_filled.astype(float), nan=0.0, posinf=0.0, neginf=0.0)
    return data.reshape(1, 100, len(required_cols))

def calculate_dynamic_thresholds(adx, strategy="trend"):
    base_up, base_down = {
        "trend": (0.60, 0.40),
        "scalp": (0.65, 0.35),
        "range": (0.55, 0.45)
    }.get(strategy, (0.55, 0.45))
    adj = (adx / 1000)
    up = max(base_up - adj, base_down + 0.05)
    down = min(base_down + adj, up - 0.05)
    return round(up, 3), round(down, 3)

def select_strategy_mode(adx, rsi, atr):
    if adx > 30:
        return "trend"
    elif adx < 15 and 40 < rsi < 60 and atr < 30:
        return "range"
    return "scalp"

def log_indicator_summary(symbol, rsi, macd, adx, ema20, ema50, atr, roc, confidence_factors):
    table = Table(title=f"Indicator Summary for {symbol}")
    table.add_column("Indicator", style="cyan")
    table.add_column("Value", style="magenta")
    table.add_column("Interpretation", style="green")

    if rsi < 30:
        table.add_row("RSI", f"{rsi:.2f}", "Near oversold zone")
    elif rsi > 70:
        table.add_row("RSI", f"{rsi:.2f}", "Near overbought zone")
    else:
        table.add_row("RSI", f"{rsi:.2f}", "Neutral")

    if macd > 0.5:
        table.add_row("MACD", f"{macd:.4f}", "Bullish")
    elif macd < -0.5:
        table.add_row("MACD", f"{macd:.4f}", "Bearish")
    else:
        table.add_row("MACD", f"{macd:.4f}", "Neutral to slightly bearish" if macd < 0 else "Neutral to slightly bullish")

    if adx >= 50:
        table.add_row("ADX", f"{adx:.2f}", "Strong trend detected")
    elif adx >= 25:
        table.add_row("ADX", f"{adx:.2f}", "Moderate trend")
    else:
        table.add_row("ADX", f"{adx:.2f}", "Weak trend")

    if abs(ema20 - ema50) / ema50 < 0.001:
        table.add_row("EMA20 vs EMA50", f"{ema20:.4f} ≈ {ema50:.4f}", "Neutral moving average crossover")
    elif ema20 > ema50:
        table.add_row("EMA20 vs EMA50", f"{ema20:.4f} > {ema50:.4f}", "Bullish trend")
    else:
        table.add_row("EMA20 vs EMA50", f"{ema20:.4f} < {ema50:.4f}", "Bearish trend")

    if roc > 1:
        table.add_row("ROC", f"{roc:.2f}%", "Strong bullish momentum")
    elif roc < -1:
        table.add_row("ROC", f"{roc:.2f}%", "Strong bearish momentum")
    elif roc > 0:
        table.add_row("ROC", f"{roc:.2f}%", "Light bullish momentum")
    elif roc < 0:
        table.add_row("ROC", f"{roc:.2f}%", "Bearish momentum")
    else:
        table.add_row("ROC", f"{roc:.2f}%", "Neutral")

    table.add_row("Confidence Factors", ", ".join(confidence_factors) if confidence_factors else "None", "")

    console.log(table)

def calculate_market_direction(symbol, signal_timestamp):
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

def should_retrain_model(model_performance=None):
    if datetime.now().weekday() == 0:
        return True
    new_data_count = get_training_data_count(since_last_train=True)
    if new_data_count >= 100:
        return True
    if model_performance and model_performance.get('accuracy', 1.0) < 0.6:
        logger.info("[Retrain] Model accuracy below 0.6, triggering retraining")
        return True
    return False

def backtest_signal(df, action, threshold):
    """Backtest signal with Sharpe ratio for profitability."""
    past_signals = df[df['close'].shift(1) > threshold if action == 'buy' else df['close'].shift(1) < threshold]
    if len(past_signals) < 5:
        logger.debug(f"[Backtest] Insufficient signals ({len(past_signals)}) for {action}")
        return False
    returns = past_signals['close'].pct_change().dropna()
    sharpe_ratio = (returns.mean() / returns.std()) * np.sqrt(252) if returns.std() != 0 else 0
    logger.debug(f"[Backtest] Sharpe ratio for {action}: {sharpe_ratio:.2f}")
    return sharpe_ratio > 0.5  # Require positive and reasonable Sharpe ratio

def check_signal(df, model, current_position, last_order_details, symbol, last_action_sent=None, config=None):
    if len(df) < 100:
        logger.debug(f"[check_signal] Not enough data for {symbol}: {len(df)} rows")
        return "hold", None, 0.0, []

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
    logger.info(f"[Strategy] Switched to {strategy_mode}, ADX: {adx:.2f}, RSI: {rsi:.2f}, ATR: {atr:.2f}, Roc: {roc:.2f} for {symbol}")

    lstm_input = prepare_lstm_input(df)
    try:
        prediction = model.predict(lstm_input, verbose=0)[0][0]
        logger.debug(f"[check_signal] LSTM prediction for {symbol}: {prediction:.4f}")
    except Exception as e:
        logger.error(f"[Prediction Error] {e} for {symbol}")
        return "hold", current_position, 0.0, []

    dynamic_up, dynamic_down = calculate_dynamic_thresholds(adx, strategy_mode)
    logger.info(f"[Thresholds] {symbol} → Prediction: {prediction:.4f}, Dynamic Down: {dynamic_down:.4f}, Dynamic Up: {dynamic_up:.4f}")
    
    trend_up = ema20 > ema50
    trend_down = ema20 < ema50
    macd_bullish = macd > signal_line
    rsi_strong = (trend_up and rsi > 50) or (trend_down and rsi < 45) or (abs(rsi - 50) > 15)
    rolling_high = df['high'].rolling(window=20).max()
    rolling_low = df['low'].rolling(window=20).min()
    breakout_up = close > (rolling_high.iloc[-1] - 0.1 * atr) if len(df) >= 20 else False
    breakout_down = close < (rolling_low.iloc[-1] + 0.1 * atr) if len(df) >= 20 else False
    bullish_divergence = (df['close'].iloc[-1] < df['close'].iloc[-3] and df['RSI'].iloc[-1] > df['RSI'].iloc[-3])
    bearish_divergence = (df['close'].iloc[-1] > df['close'].iloc[-3] and df['RSI'].iloc[-1] < df['RSI'].iloc[-3])

    confidence_factors = []
    if prediction > 0.55 or prediction < 0.45:
        confidence_factors.append("LSTM strong")
    if (trend_up and macd > signal_line) or (trend_down and macd < signal_line):
        confidence_factors.append("MACD aligned")
    if rsi > 50 or rsi < 50:
        confidence_factors.append("RSI strong")
    if (trend_up and ema20 > ema50) or (trend_down and ema20 < ema50):
        confidence_factors.append("EMA trend")
    if abs(roc) > 0.5:
        confidence_factors.append("ROC momentum")
    if breakout_up or breakout_down:
        confidence_factors.append("Breakout detected")

    log_indicator_summary(symbol, rsi, macd, adx, ema20, ema50, atr, roc, confidence_factors)

    action = "hold"
    new_position = None
    signal_timestamp = int(df.index[-1].timestamp() * 1000)
    logger.debug(f"[Timestamp] Using {signal_timestamp} ({datetime.utcfromtimestamp(signal_timestamp/1000)})")

    if config is None:
        logger.error(f"[check_signal] Config is None for {symbol}, cannot calculate quantity")
        return "hold", current_position, 0.0, []

    capital = config["binance"].get("capital", 100.0)
    leverage = config["binance"].get("leverage", 50.0)
    quantity = (capital * leverage) / close if close > 0 else 0.0

    # Adjusted risk/reward for low ATR assets
    expected_pnl = atr * 2 if atr > 1 else close * 0.01  # 1% of price for low ATR
    risk_reward_ratio = expected_pnl / atr if atr > 0 else 0
    if len(confidence_factors) < 3 or risk_reward_ratio < 1.5:
        logger.info(f"[Signal Rejected] {symbol} - Insufficient confidence ({len(confidence_factors)}/6) or risk/reward ratio ({risk_reward_ratio:.2f} < 1.5)")
        return "hold", current_position, 0.0, confidence_factors

    # Validation du signal via backtest
    if action in ["buy", "sell"] and not backtest_signal(df, action, dynamic_up if action == 'buy' else dynamic_down):
        logger.info(f"[Signal Rejected] {symbol} - Backtest failed for {action}")
        return "hold", current_position, 0.0, confidence_factors

    if strategy_mode == "scalp" and rsi_strong:
        if prediction > dynamic_up and roc > 0.5:
            action = "sell"
            new_position = "short"
        elif prediction < dynamic_down and roc < -0.5:
            action = "buy"
            new_position = "long"
    elif strategy_mode == "trend":
        if trend_up and macd_bullish and prediction > dynamic_up and roc > 0.5:
            action = "buy"
            new_position = "long"
        elif trend_down and not macd_bullish and prediction < dynamic_down and roc < -0.5:
            action = "sell"
            new_position = "short"
    elif strategy_mode == "range":
        range_high = rolling_high.iloc[-1]
        range_low = rolling_low.iloc[-1]
        if close <= (range_low + 0.1 * atr) and prediction > dynamic_up and roc > 0.3:
            action = "buy"
            new_position = "long"
        elif close >= (range_high - 0.1 * atr) and prediction < dynamic_down and roc < -0.3:
            action = "sell"
            new_position = "short"

    if bullish_divergence and prediction > 0.65 and roc > 0.5:
        action = "buy"
        new_position = "long"
    elif bearish_divergence and prediction < 0.35 and roc < -0.5:
        action = "sell"
        new_position = "short"

    if current_position == "long" and (trend_down or not macd_bullish or roc < -0.5):
        action = "close_buy"
        new_position = None
    elif current_position == "short" and (trend_up or macd_bullish or roc > 0.5):
        action = "close_sell"
        new_position = None
    elif current_position is None and action in ["buy", "sell"]:
        new_position = {
            "side": "long" if action == "buy" else "short",
            "quantity": quantity,
            "price": close,
            "trade_id": str(signal_timestamp)
        }

    logger.debug(f"[check_signal] Action for {symbol}: {action}, Confidence: {len(confidence_factors)}/6, Prediction: {prediction:.4f}")
    if action != "hold":
        logger.info(f"[Signal] {symbol} - Action: {action}, Confidence: {len(confidence_factors)}/6, Prediction: {prediction:.4f}, Risk/Reward: {risk_reward_ratio:.2f}")
        send_telegram_alert(f"[Signal] {symbol} - Action: {action}, Confidence: {len(confidence_factors)}/6, Prediction: {prediction:.4f}, Risk/Reward: {risk_reward_ratio:.2f}")

    if last_action_sent is not None and isinstance(last_action_sent, tuple) and action == last_action_sent[0]:
        logger.info(f"[Anti-Repeat] Signal {action} ignored for {symbol} as it was sent previously.")
        return "hold", current_position, 0.0, []

    new_position = None
    if action == "buy":
        new_position = {
            "side": "long",
            "quantity": quantity,
            "price": close,
            "trade_id": str(signal_timestamp)
        }
    elif action == "sell":
        new_position = {
            "side": "short",
            "quantity": quantity,
            "price": close,
            "trade_id": str(signal_timestamp)
        }
    elif action in ["close_buy", "close_sell"]:
        new_position = None

    try:
        indicators = {
            "rsi": round(rsi, 2),
            "macd": round(macd, 4),
            "adx": round(adx, 2),
            "roc": round(roc, 2),
            "ema20": round(ema20, 4),
            "ema50": round(ema50, 4),
            "atr": round(atr, 4),
            "strategy_mode": strategy_mode
        }
        market_context = {
            "trend_up": bool(trend_up),
            "trend_down": bool(trend_down),
            "macd_bullish": bool(macd_bullish),
            "breakout_up": bool(breakout_up),
            "breakout_down": bool(breakout_down)
        }
        check_timestamp = int((datetime.now() + timedelta(minutes=5)).timestamp() * 1000)
        success = insert_training_data(symbol, signal_timestamp, indicators, market_context, prediction=prediction, action=action, price=close)
        if success:
            logger.info(f"[Performance Tracking] Stored training data for {symbol} at {signal_timestamp}")
            check_time = datetime.now() + timedelta(minutes=5)
            logger.info(f"[Performance Tracking] Will verify market direction at {check_time.strftime('%H:%M')}")
        else:
            logger.error(f"[Performance Tracking] Failed to store training data for {symbol} at {signal_timestamp}")
            raise Exception("Training data insertion failed")
    except Exception as e:
        logger.error(f"[Performance Tracking] Error storing training data for {symbol}: {str(e)}")
        raise

    weights = {
        "LSTM strong": 0.30,
        "MACD aligned": 0.20,
        "RSI strong": 0.20,
        "EMA trend": 0.15,
        "ROC momentum": 0.15,
        "Breakout detected": 0.10
    }
    confidence = sum(weights[f] for f in confidence_factors if f in weights)

    if action != "hold":
        try:
            insert_signal(symbol, signal_timestamp, action.lower(), close, confidence, strategy_mode)
            logger.info(f"[Signal Stored] {action} at {close} for {symbol} with confidence {confidence:.2f}")
        except Exception as e:
            logger.error(f"[Signal Stored] Failed to store signal for {symbol}: {str(e)}")
            raise
    else:
        logger.info(f"[Hold Action] Stored hold action for {symbol} at {signal_timestamp} with prediction {prediction:.4f}")

    logger.info(f"[Confidence Score] {symbol} → {confidence:.2f} | Factors: {', '.join(confidence_factors) if confidence_factors else 'None'}")

    return action, new_position, confidence, confidence_factors