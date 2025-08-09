"""
Signal Generator Module - Core Trading Signal Generation

This module generates trading signals using technical indicators and an LSTM model.
It includes strategy selection, confidence scoring, and risk management features.

Organized into logical classes and functions for better maintainability.
"""

import logging
import numpy as np
import pandas as pd
import talib
from datetime import datetime, timedelta
from scipy.stats import norm
from rich.console import Console
from rich.table import Table
from src.database.db_handler import insert_signal, insert_training_data, get_future_prices, get_training_data_count
from src.monitoring.alerting import send_telegram_alert

# Initialize logging and console
logger = logging.getLogger(__name__)
console = Console()

class DataPreprocessor:
    """Handles data preprocessing for LSTM model input."""

    @staticmethod
    def normalize_lstm_input(arr):
        """Normalize input array for LSTM model."""
        mean = arr.mean(axis=1, keepdims=True)
        std = arr.std(axis=1, keepdims=True) + 1e-9
        return (arr - mean) / std

    @staticmethod
    def prepare_lstm_input(df, window=100):
        """Prepare input data for LSTM model."""
        required_cols = ['close', 'volume', 'RSI', 'MACD', 'ADX']
        df = df.copy()
        # Fill missing columns if needed
        for col in required_cols:
            if col not in df.columns:
                df[col] = 0.0
        data = df[required_cols].tail(window).values.astype(float)
        if np.isnan(data).any():
            data = pd.DataFrame(data, columns=required_cols).ffill().bfill().values
        data = DataPreprocessor.normalize_lstm_input(data.T).T
        return data.reshape(1, window, len(required_cols))

class StrategySelector:
    """Determines the appropriate trading strategy based on market conditions."""
    
    @staticmethod
    def select_strategy_mode(adx, rsi, atr):
        """Select strategy mode based on ADX, RSI, and ATR."""
        if adx > 30:
            return "trend"
        elif adx < 15 and 40 < rsi < 60 and atr < 30:
            return "range"
        return "scalp"

    @staticmethod
    def calculate_dynamic_thresholds(adx, strategy="trend"):
        """Calculate dynamic thresholds for signal generation."""
        base_up, base_down = {
            "trend": (0.60, 0.40),
            "scalp": (0.65, 0.35),
            "range": (0.55, 0.45)
        }.get(strategy, (0.55, 0.45))
        adj = (adx / 1000)
        up = max(base_up - adj, base_down + 0.05)
        down = min(base_down + adj, up - 0.05)
        return round(up, 3), round(down, 3)

class IndicatorAnalyzer:
    """Analyzes technical indicators and logs summaries."""
    
    @staticmethod
    def log_indicator_summary(symbol, rsi, macd, adx, ema20, ema50, atr, roc, confidence_factors):
        """Log a summary of technical indicators in a formatted table."""
        table = Table(title=f"Indicator Summary for {symbol}")
        table.add_column("Indicator", style="cyan")
        table.add_column("Value", style="magenta")
        table.add_column("Interpretation", style="green")

        # RSI interpretation
        rsi_status = "Neutral"
        if rsi < 30:
            rsi_status = "Near oversold zone"
        elif rsi > 70:
            rsi_status = "Near overbought zone"
        table.add_row("RSI", f"{rsi:.2f}", rsi_status)

        # MACD interpretation
        macd_status = "Neutral to slightly bullish" if macd >= 0 else "Neutral to slightly bearish"
        if macd > 0.5:
            macd_status = "Bullish"
        elif macd < -0.5:
            macd_status = "Bearish"
        table.add_row("MACD", f"{macd:.4f}", macd_status)

        # ADX interpretation
        adx_status = "Weak trend"
        if adx >= 50:
            adx_status = "Strong trend detected"
        elif adx >= 25:
            adx_status = "Moderate trend"
        table.add_row("ADX", f"{adx:.2f}", adx_status)

        # EMA20 vs EMA50
        ema_status = "Neutral moving average crossover"
        if abs(ema20 - ema50) / ema50 < 0.001:
            ema_status = "Neutral moving average crossover"
        elif ema20 > ema50:
            ema_status = "Bullish trend"
        else:
            ema_status = "Bearish trend"
        table.add_row("EMA20 vs EMA50", f"{ema20:.4f} ≈ {ema50:.4f}", ema_status)

        # ROC interpretation
        roc_status = "Neutral"
        if roc > 1:
            roc_status = "Strong bullish momentum"
        elif roc < -1:
            roc_status = "Strong bearish momentum"
        elif roc > 0:
            roc_status = "Light bullish momentum"
        elif roc < 0:
            roc_status = "Bearish momentum"
        table.add_row("ROC", f"{roc:.2f}%", roc_status)

        table.add_row("Confidence Factors", ", ".join(confidence_factors) if confidence_factors else "None", "")
        console.log(table)

class RiskManager:
    """Handles risk management calculations."""
    
    @staticmethod
    def historical_var(returns, alpha=0.05):
        """Calculate Historical Value at Risk (VaR)."""
        if len(returns) < 10:
            return None
        return np.percentile(returns.dropna(), 100 * alpha)

    @staticmethod
    def parametric_var(returns, alpha=0.05):
        """Calculate Parametric Value at Risk (VaR)."""
        mu = returns.mean()
        sigma = returns.std()
        return mu + sigma * norm.ppf(alpha)

    @staticmethod
    def kelly_fraction(win_prob, win_loss_ratio):
        """Calculate Kelly Criterion fraction for position sizing."""
        if win_loss_ratio <= 0:
            return 0.0
        return max(0.0, (win_prob - (1 - win_prob) / win_loss_ratio))

class SignalValidator:
    """Validates trading signals through backtesting."""
    
    @staticmethod
    def backtest_signal(df, action, threshold, lookback=252):
        """Backtest signal with Sharpe ratio using historical windows."""
        if action == 'buy':
            mask = df['close'].shift(1) > threshold
        else:
            mask = df['close'].shift(1) < threshold

        past_signals = df[mask].copy()
        if len(past_signals) < 10:
            logger.debug(f"[Backtest] Insufficient signals ({len(past_signals)}) for {action}")
            return False

        returns = past_signals['close'].pct_change().shift(-1).dropna()
        if returns.empty or returns.std() == 0:
            return False

        sharpe_ratio = (returns.mean() / returns.std()) * np.sqrt(252)
        logger.debug(f"[Backtest] Sharpe ratio for {action}: {sharpe_ratio:.2f} ({len(returns)} samples)")
        return sharpe_ratio > 0.5

class PerformanceTracker:
    """Tracks signal performance and model retraining conditions."""
    
    @staticmethod
    def calculate_market_direction(symbol, signal_timestamp):
        """Calculate market direction based on future price movements."""
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

    @staticmethod
    def should_retrain_model(model_performance=None):
        """Determine if model retraining is needed."""
        if datetime.now().weekday() == 0:
            return True
        new_data_count = get_training_data_count(since_last_train=True)
        if new_data_count >= 100:
            return True
        if model_performance and model_performance.get('accuracy', 1.0) < 0.6:
            logger.info("[Retrain] Model accuracy below 0.6, triggering retraining")
            return True
        return False

class TechnicalIndicators:
    """Calculates additional technical indicators."""
    
    @staticmethod
    def add_bollinger(df, period=20, nbdev=2):
        """Add Bollinger Bands to the dataframe."""
        df['BB_mid'] = df['close'].rolling(window=period).mean()
        df['BB_std'] = df['close'].rolling(window=period).std()
        df['BB_upper'] = df['BB_mid'] + nbdev * df['BB_std']
        df['BB_lower'] = df['BB_mid'] - nbdev * df['BB_std']
        return df

    @staticmethod
    def calculate_vwap(df):
        """Calculate Volume Weighted Average Price (VWAP)."""
        tp = (df['high'] + df['low'] + df['close']) / 3.0
        df['vwap_num'] = (tp * df['volume']).cumsum()
        df['vwap_den'] = df['volume'].cumsum()
        df['VWAP'] = df['vwap_num'] / df['vwap_den']
        return df

def adaptive_weights(symbol, window=100):
    """Calculate adaptive weights for confidence scoring (placeholder)."""
    return {
        "LSTM strong": 0.25,
        "MACD aligned": 0.20,
        "RSI strong": 0.20,
        "EMA trend": 0.15,
        "ROC momentum": 0.12,
        "Breakout detected": 0.08
    }

def check_signal(df, model, current_position, last_order_details, symbol, last_action_sent=None, config=None):
    """
    Main function to generate trading signals.
    Returns: (action, new_position, confidence, confidence_factors)
    """
    # ✅ Compatibilité : extraire le side si current_position est un dict
    if isinstance(current_position, dict):
        current_side = current_position.get("side")
    else:
        current_side = current_position

    if len(df) < 100:
        logger.debug(f"[check_signal] Not enough data for {symbol}: {len(df)} rows")
        return "hold", current_position, 0.0, []

    # === Extraction des indicateurs ===
    rsi = df['RSI'].iloc[-1]
    macd = df['MACD'].iloc[-1]
    signal_line = df['MACD_signal'].iloc[-1]
    adx = df['ADX'].iloc[-1]
    ema20 = df['EMA20'].iloc[-1]
    ema50 = df['EMA50'].iloc[-1]
    atr = df['ATR'].iloc[-1]
    close = df['close'].iloc[-1]
    roc = talib.ROC(df['close'], timeperiod=5).iloc[-1] * 100

    # === Sélection de stratégie ===
    strategy_mode = StrategySelector.select_strategy_mode(adx, rsi, atr)
    logger.info(f"[Strategy] Switched to {strategy_mode}, ADX: {adx:.2f}, RSI: {rsi:.2f}, ATR: {atr:.2f}, Roc: {roc:.2f} for {symbol}")

    # === Prédiction LSTM ===
    lstm_input = DataPreprocessor.prepare_lstm_input(df)
    try:
        prediction = model.predict(lstm_input, verbose=0)[0][0].item()
        logger.debug(f"[check_signal] LSTM prediction for {symbol}: {prediction:.4f}")
    except Exception as e:
        logger.error(f"[Prediction Error] {e} for {symbol}")
        return "hold", current_position, 0.0, []

    # === Seuils dynamiques ===
    dynamic_up, dynamic_down = StrategySelector.calculate_dynamic_thresholds(adx, strategy_mode)
    logger.info(f"[Thresholds] {symbol} → Prediction: {prediction:.4f}, Dynamic Down: {dynamic_down:.4f}, Dynamic Up: {dynamic_up:.4f}")

    # === Conditions de marché ===
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

    # === Facteurs de confiance ===
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

    IndicatorAnalyzer.log_indicator_summary(symbol, rsi, macd, adx, ema20, ema50, atr, roc, confidence_factors)

    # === Init action/position ===
    action = "hold"
    new_position = current_position
    signal_timestamp = int(df.index[-1].timestamp() * 1000)

    # === Validation config ===
    if config is None:
        logger.error(f"[check_signal] Config is None for {symbol}")
        return "hold", current_position, 0.0, confidence_factors

    capital = config["binance"].get("capital", 100.0)
    leverage = config["binance"].get("leverage", 50.0)
    quantity = (capital * leverage) / close if close > 0 else 0.0

    expected_pnl = atr * 2 if atr > 1 else close * 0.01
    risk_reward_ratio = expected_pnl / atr if atr > 0 else 0
    if len(confidence_factors) < 3 or risk_reward_ratio < 1.5:
        return "hold", current_position, 0.0, confidence_factors

    # === Logique de signaux ===
    # --- Signal logic (buy/sell/close) ---
    if strategy_mode == "scalp" and rsi_strong:
        if prediction > dynamic_up and roc > 0.5:
            action = "sell"
            new_position = {"side": "short", "quantity": quantity, "price": close, "trade_id": str(signal_timestamp)}
        elif prediction < dynamic_down and roc < -0.5:
            action = "buy"
            new_position = {"side": "long", "quantity": quantity, "price": close, "trade_id": str(signal_timestamp)}
    elif strategy_mode == "trend":
        if trend_up and macd_bullish and prediction > dynamic_up and roc > 0.5:
            action = "buy"
            new_position = {"side": "long", "quantity": quantity, "price": close, "trade_id": str(signal_timestamp)}
        elif trend_down and not macd_bullish and prediction < dynamic_down and roc < -0.5:
            action = "sell"
            new_position = {"side": "short", "quantity": quantity, "price": close, "trade_id": str(signal_timestamp)}
    elif strategy_mode == "range":
        range_high = rolling_high.iloc[-1]
        range_low = rolling_low.iloc[-1]
        if close <= (range_low + 0.1 * atr) and prediction > dynamic_up and roc > 0.3:
            action = "buy"
            new_position = {"side": "long", "quantity": quantity, "price": close, "trade_id": str(signal_timestamp)}
        elif close >= (range_high - 0.1 * atr) and prediction < dynamic_down and roc < -0.3:
            action = "sell"
            new_position = {"side": "short", "quantity": quantity, "price": close, "trade_id": str(signal_timestamp)}

    # Divergence signals
    if bullish_divergence and prediction > 0.65 and roc > 0.5:
        action = "buy"
        new_position = {"side": "long", "quantity": quantity, "price": close, "trade_id": str(signal_timestamp)}
    elif bearish_divergence and prediction < 0.35 and roc < -0.5:
        action = "sell"
        new_position = {"side": "short", "quantity": quantity, "price": close, "trade_id": str(signal_timestamp)}

    # Position closure
    if current_side == "long" and (trend_down or not macd_bullish or roc < -0.5):
        action = "close_buy"
        new_position = None
    elif current_side == "short" and (trend_up or macd_bullish or roc > 0.5):
        action = "close_sell"
        new_position = None

    # Backtest validation
    if action in ["buy", "sell"] and not SignalValidator.backtest_signal(df, action, dynamic_up if action == 'buy' else dynamic_down):
        logger.info(f"[Signal Rejected] {symbol} - Backtest failed for {action}")
        return "hold", current_position, 0.0, confidence_factors

    # Anti-repeat check
    if last_action_sent is not None and isinstance(last_action_sent, tuple) and action == last_action_sent[0]:
        logger.info(f"[Anti-Repeat] Signal {action} ignored for {symbol} as it was sent previously.")
        return "hold", current_position, 0.0, confidence_factors

    # Calculate confidence
    weights = adaptive_weights(symbol)
    confidence = sum(weights[f] for f in confidence_factors if f in weights)

    # Log and store signal
    if action != "hold":
        logger.info(f"[Signal] {symbol} - Action: {action}, Confidence: {len(confidence_factors)}/6, Prediction: {prediction:.4f}, Risk/Reward: {risk_reward_ratio:.2f}")
        send_telegram_alert(f"[Signal] {symbol} - Action: {action}, Confidence: {len(confidence_factors)}/6, Prediction: {prediction:.4f}, Risk/Reward: {risk_reward_ratio:.2f}")
        try:
            insert_signal(symbol, signal_timestamp, action.lower(), close, confidence, strategy_mode)
            logger.info(f"[Signal Stored] {action} at {close} for {symbol} with confidence {confidence:.4f}")
        except Exception as e:
            logger.error(f"[Signal Storage Error] {e} for {symbol}")

    # === Retour final garanti ===
    return action, new_position, confidence, confidence_factors