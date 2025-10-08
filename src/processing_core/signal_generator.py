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
from binance.client import Client

# === Ajout : Configuration dynamique des indicateurs selon le timeframe ===
class TimeframeConfig:
    """
    Configure indicator parameters dynamically based on timeframe.
    """
    DEFAULTS = {
        "1m":  {"rsi": 7, "macd": (5, 13, 4), "adx": 7, "ema": (5, 20), "atr": 7,  "roc": 4},
        "5m":  {"rsi": 9, "macd": (8, 21, 5), "adx": 10, "ema": (9, 26), "atr": 10, "roc": 5},
        "15m": {"rsi": 9, "macd": (8, 21, 5), "adx": 10, "ema": (9, 26), "atr": 10, "roc": 6},
        "1h":  {"rsi": 14, "macd": (12, 26, 9), "adx": 14, "ema": (20, 50), "atr": 14, "roc": 10},
        "4h":  {"rsi": 14, "macd": (12, 26, 9), "adx": 14, "ema": (20, 50), "atr": 14, "roc": 14},
        "1d":  {"rsi": 14, "macd": (12, 26, 9), "adx": 14, "ema": (20, 50), "atr": 14, "roc": 14},
    }

    def __init__(self, timeframe="15m"):
        self.timeframe = timeframe.lower()
        self.params = self.DEFAULTS.get(self.timeframe, self.DEFAULTS["15m"])
        logging.info(f"[TimeframeConfig] Loaded parameters for {self.timeframe}: {self.params}")

    def get_params(self):
        """Return indicator parameters."""
        return self.params

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

def get_available_margin(api_key, api_secret):
    """Retourne la marge disponible en USDT sur Binance Futures."""
    try:
        client = Client(api_key, api_secret)
        account_info = client.futures_account()
        balance = next((float(b['availableBalance']) for b in account_info['assets'] if b['asset'] == 'USDT'), 0.0)
        return balance
    except Exception as e:
        logger.error(f"[Binance API] Erreur récupération marge : {e}")
        return 0.0

def check_signal(df, model, current_position, last_order_details, symbol, last_action_sent=None, config=None):
    """
    Optimized signal checker with dynamic timeframe configuration.
    """
    # === Timeframe dynamique ===
    timeframe = config.get("timeframe", "15m") if config else "15m"
    tf_params = TimeframeConfig(timeframe).get_params()

    if isinstance(current_position, dict):
        current_side = current_position.get("side")
    else:
        current_side = current_position

    if len(df) < 100:
        logger.debug(f"[check_signal] Not enough data for {symbol}: {len(df)} rows")
        return "hold", current_position, 0.0, []

    # === Extraction indicateurs dynamiques selon le timeframe ===
    rsi = talib.RSI(df['close'], timeperiod=tf_params["rsi"]).iloc[-1]
    macd_arr, signal_arr, _ = talib.MACD(
        df['close'],
        fastperiod=tf_params["macd"][0],
        slowperiod=tf_params["macd"][1],
        signalperiod=tf_params["macd"][2]
    )
    macd = macd_arr.iloc[-1]
    signal_line = signal_arr.iloc[-1]
    adx = talib.ADX(df['high'], df['low'], df['close'], timeperiod=tf_params["adx"]).iloc[-1]
    ema_short = talib.EMA(df['close'], timeperiod=tf_params["ema"][0]).iloc[-1]
    ema_long = talib.EMA(df['close'], timeperiod=tf_params["ema"][1]).iloc[-1]
    atr = max(talib.ATR(df['high'], df['low'], df['close'], timeperiod=tf_params["atr"]).iloc[-1], df['close'].iloc[-1] * 0.005)
    close = df['close'].iloc[-1]
    roc = talib.ROC(df['close'], timeperiod=tf_params["roc"]).iloc[-1] * 100

    # === Détection d'un renversement imminent ===
    reversal_detected = False
    reversal_strength = 0.0

    # Divergence RSI/price
    rsi_prev = df['RSI'].iloc[-2]
    price_prev = df['close'].iloc[-2]
    if (close > price_prev and rsi < rsi_prev - 5) or (close < price_prev and rsi > rsi_prev + 5):
        reversal_detected = True
        reversal_strength += 0.4

    # MACD affaibli malgré la tendance
    if abs(macd) < abs(df['MACD'].iloc[-2]) and (macd * signal_line) < 0:
        reversal_detected = True
        reversal_strength += 0.3

    # Volume en baisse pendant poursuite du mouvement
    if df['volume'].iloc[-1] < df['volume'].rolling(10).mean().iloc[-1] * 0.8:
        reversal_detected = True
        reversal_strength += 0.2

    # LSTM perd confiance dans la direction actuelle
    lstm_input = DataPreprocessor.prepare_lstm_input(df)
    try:
        prediction = model.predict(lstm_input, verbose=0)[0][0].item()
        logger.debug(f"[Prediction] LSTM={prediction:.4f} for {symbol}")
    except Exception as e:
        logger.error(f"[Prediction Error] {e} for {symbol}")
        return "hold", current_position, 0.0, []

    if 0.45 < prediction < 0.55:
        reversal_detected = True
        reversal_strength += 0.3

    if reversal_detected:
        logger.info(f"[Reversal] ⚠️ Possible trend reversal on {symbol} | Strength={reversal_strength:.2f}")

    # === Sélection stratégie ===
    strategy_mode = StrategySelector.select_strategy_mode(adx, rsi, atr)
    logger.info(f"[Strategy] {strategy_mode} mode for {symbol} (ADX={adx:.2f}, RSI={rsi:.2f}, ATR={atr:.4f})")

    # === Dynamic thresholds (plus souples) ===
    dynamic_up, dynamic_down = StrategySelector.calculate_dynamic_thresholds(adx, strategy_mode)
    dynamic_up = max(dynamic_up, 0.52)
    dynamic_down = min(dynamic_down, 0.48)

    # === Conditions marché ===
    trend_up = ema_short > ema_long
    trend_down = ema_short < ema_long
    macd_bullish = macd > signal_line
    rolling_high = df['high'].rolling(window=20).max()
    rolling_low = df['low'].rolling(window=20).min()
    breakout_up = close > (rolling_high.iloc[-1] - 0.1 * atr) if len(df) >= 20 else False
    breakout_down = close < (rolling_low.iloc[-1] + 0.1 * atr) if len(df) >= 20 else False

    # === Facteurs de confiance ===
    confidence_factors = []
    if prediction > 0.52 or prediction < 0.48:
        confidence_factors.append("LSTM strong")
    if (trend_up and macd > signal_line) or (trend_down and macd < signal_line):
        confidence_factors.append("MACD aligned")
    if rsi > 55 or rsi < 45:
        confidence_factors.append("RSI strong")
    if breakout_up or breakout_down:
        confidence_factors.append("Breakout detected")

    IndicatorAnalyzer.log_indicator_summary(symbol, rsi, macd, adx, ema_short, ema_long, atr, roc, confidence_factors)

    # === Config API ===
    if config is None:
        logger.error("[check_signal] Missing config")
        return "hold", current_position, 0.0, []

    api_key = config["binance"]["api_key"]
    api_secret = config["binance"]["api_secret"]
    available_margin = get_available_margin(api_key, api_secret)
    leverage = config["binance"].get("leverage", 30)

    # Position sizing avec Kelly Criterion
    max_capital = 0.95 * available_margin
    raw_qty = (max_capital * leverage) / close if close > 0 else 0.0
    kelly_fraction = RiskManager.kelly_fraction(win_prob=0.55, win_loss_ratio=2)
    quantity = raw_qty * (kelly_fraction if kelly_fraction > 0 else 0.1)   # min 10%

    # === Signal generation ===
    action = "hold"
    new_position = current_position
    signal_timestamp = int(df.index[-1].timestamp() * 1000)

    # Risk/Reward check (assoupli)
    expected_pnl = atr * 2
    risk_reward_ratio = expected_pnl / atr
    if len(confidence_factors) < 2 or risk_reward_ratio < 1.2:
        return "hold", current_position, 0.0, confidence_factors

    # === Gestion proactive du renversement ===
    if reversal_detected and reversal_strength > 0.5:
        if current_side == "long":
            action = "close_buy"
            logger.info(f"[Reversal] Closing LONG early on {symbol} (anticipating trend change)")
            new_position = None
        elif current_side == "short":
            action = "close_sell"
            logger.info(f"[Reversal] Closing SHORT early on {symbol} (anticipating trend change)")
            new_position = None
        else:
            # Si aucune position ouverte, on peut anticiper un inversement
            if trend_up and macd < 0 and rsi < 55:
                action = "sell"
                logger.info(f"[Reversal Entry] {symbol} potential SHORT (pre-reversal)")
                new_position = {"side": "short", "quantity": quantity, "price": close, "trade_id": str(signal_timestamp)}
            elif trend_down and macd > 0 and rsi > 45:
                action = "buy"
                logger.info(f"[Reversal Entry] {symbol} potential LONG (pre-reversal)")
                new_position = {"side": "long", "quantity": quantity, "price": close, "trade_id": str(signal_timestamp)}

    # Logique par stratégie (si pas de reversal)
    if action == "hold":
        if strategy_mode == "trend":
            if trend_up and macd_bullish and prediction > dynamic_up:
                action = "buy"
                new_position = {"side": "long", "quantity": quantity, "price": close, "trade_id": str(signal_timestamp)}
            elif trend_down and not macd_bullish and prediction < dynamic_down:
                action = "sell"
                new_position = {"side": "short", "quantity": quantity, "price": close, "trade_id": str(signal_timestamp)}
        elif strategy_mode == "range":
            if close <= (rolling_low.iloc[-1] + 0.1 * atr) and prediction > dynamic_up:
                action = "buy"
                new_position = {"side": "long", "quantity": quantity, "price": close, "trade_id": str(signal_timestamp)}
            elif close >= (rolling_high.iloc[-1] - 0.1 * atr) and prediction < dynamic_down:
                action = "sell"
                new_position = {"side": "short", "quantity": quantity, "price": close, "trade_id": str(signal_timestamp)}
        elif strategy_mode == "scalp":
            if prediction > dynamic_up and roc > 0.5:
                action = "buy"   
                new_position = {"side": "long", "quantity": quantity, "price": close, "trade_id": str(signal_timestamp)}
            elif prediction < dynamic_down and roc < -0.5:
                action = "sell"  
                new_position = {"side": "short", "quantity": quantity, "price": close, "trade_id": str(signal_timestamp)}


    # Fermeture de position
    if current_side == "long" and (trend_down or roc < -1):
        action = "close_buy"
        new_position = None
    elif current_side == "short" and (trend_up or roc > 1):
        action = "close_sell"
        new_position = None

    # Backtest → indicatif
    if action in ["buy", "sell"]:
        if not SignalValidator.backtest_signal(df, action, dynamic_up if action == "buy" else dynamic_down):
            logger.info(f"[Signal Adjusted] {symbol} - Backtest not confirmed → confiance réduite")
            confidence_factors.append("Backtest weak")

    # Anti-repeat
    if last_action_sent and isinstance(last_action_sent, tuple) and action == last_action_sent[0]:
        logger.info(f"[Anti-Repeat] Ignored {action} for {symbol}")
        return "hold", current_position, 0.0, confidence_factors

    # Calcul confiance pondérée
    weights = adaptive_weights(symbol)
    confidence = sum(weights.get(f, 0.05) for f in confidence_factors)

    # Réduction de confiance si contradiction détectée
    if (action == "buy" and trend_down) or (action == "sell" and trend_up):
        confidence *= 0.6
        logger.info(f"[Confidence Adjustment] Contradiction trend → confiance réduite ({confidence:.2f})")

    # Sauvegarde dans training_data pour le tracker
    try:
        insert_training_data(
            symbol=symbol,
            timestamp=signal_timestamp,
            indicators={
                "rsi": rsi,
                "macd": macd,
                "adx": adx,
                "ema_short": ema_short,
                "ema_long": ema_long,
                "atr": atr
            },
            market_context={
                "strategy": strategy_mode,
                "confidence_factors": confidence_factors
            },
            prediction=prediction,
            action=action.lower(),
            price=close
        )
    except Exception as e:
        logger.error(f"[Training Insert Error] {e}")

    # Stockage et alerte
    if action != "hold":
        logger.info(f"[Signal] {symbol} → {action.upper()} at {close:.4f} | Conf: {confidence:.2f} | P={prediction:.4f} | R/R={risk_reward_ratio:.2f}")
        send_telegram_alert(f"[Signal] {symbol} → {action.upper()} at {close:.4f} | Conf: {confidence:.2f} | P={prediction:.4f} | R/R={risk_reward_ratio:.2f}")
        try:
            insert_signal(symbol, signal_timestamp, action.lower(), close, confidence, strategy_mode)
        except Exception as e:
            logger.error(f"[DB Error] {e}")

    return action, new_position, confidence, confidence_factors