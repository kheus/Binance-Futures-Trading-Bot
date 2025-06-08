import numpy as np
import logging
from src.processing_core.indicators import calculate_indicators
from src.processing_core.lstm_model import SEQ_LEN
from src.processing_core.breakout_detector import detect_breakout

logger = logging.getLogger(__name__)

def check_signal(df, model, position_type=None, last_order_details=None):
    if len(df) < 51 or model is None:
        logger.warning("[Error] Insufficient data or model is None")
        return None, position_type

    try:
        df = calculate_indicators(df)
        last = df.iloc[-1]
        close = df["close"].iloc[-1]

        # Log indicators
        logger.info(f"[Indicators] RSI: {last['RSI']:.2f}, MACD: {last['MACD']:.2f}, "
                    f"Signal: {last['MACD_signal']:.2f}, ADX: {last['ADX']:.2f}, "
                    f"EMA20: {last['EMA20']:.2f}, EMA50: {last['EMA50']:.2f}, ATR: {last['ATR']:.2f}")

        # Stop-loss logic
        if last_order_details:
            stop_loss = last_order_details["stop_loss"]
            if position_type == "buy" and close <= stop_loss:
                logger.warning(f"[Stop-Loss] Triggered for BUY — Close: {close} <= SL: {stop_loss}")
                return "close_buy", None
            elif position_type == "sell" and close >= stop_loss:
                logger.warning(f"[Stop-Loss] Triggered for SELL — Close: {close} >= SL: {stop_loss}")
                return "close_sell", None

        # Exit conditions
        if position_type == "buy":
            adx_weak = last["ADX"] < 20 and last["MACD"] < last["MACD_signal"]
            rsi_exit = last["RSI"] > 70
            macd_cross = last["MACD"] < last["MACD_signal"]
            logger.info(f"[Exit BUY] Weak Trend: {'Yes' if adx_weak else 'No'}, "
                        f"RSI > 70: {'Yes' if rsi_exit else 'No'}, "
                        f"MACD < Signal: {'Yes' if macd_cross else 'No'}")
            if adx_weak or rsi_exit or macd_cross:
                return "close_buy", None

        elif position_type == "sell":
            adx_weak = last["ADX"] < 20 and last["MACD"] > last["MACD_signal"]
            rsi_exit = last["RSI"] < 30
            macd_cross = last["MACD"] > last["MACD_signal"]
            logger.info(f"[Exit SELL] Weak Trend: {'Yes' if adx_weak else 'No'}, "
                        f"RSI < 30: {'Yes' if rsi_exit else 'No'}, "
                        f"MACD > Signal: {'Yes' if macd_cross else 'No'}")
            if adx_weak or rsi_exit or macd_cross:
                return "close_sell", None

        # Block new entries if in position
        if position_type is not None:
            logger.info("[Position] Already open. No new entry signal.")
            return None, position_type

        # Entry logic
        trend_up = last["EMA20"] > last["EMA50"]
        macd_bullish = last["MACD"] > last["MACD_signal"]
        rsi_strong = last["RSI"] > 50
        adx_strong = last["ADX"] > 20
        trend_down = last["EMA20"] < last["EMA50"] and last["MACD"] < last["MACD_signal"] and last["RSI"] < 40 and adx_strong
        breakout = detect_breakout(df)

        logger.info(f"[Conditions] MACD Bullish: {macd_bullish}, RSI Strong: {rsi_strong}, ADX Strong: {adx_strong}")
        logger.info(f"[Trend Up] {'Yes' if trend_up else 'No'}, Breakout: {breakout or 'None'}")
        logger.info(f"[Trend Down] {'Yes' if trend_down else 'No'}, Breakout: {breakout or 'None'}")

        # Volatility filter
        if last["ATR"] < 20:
            logger.warning(f"[Volatility] ATR too low: {last['ATR']} < 20, No trade")
            return None, position_type

        # LSTM prediction
        seq_data = df[["close", "volume", "RSI", "MACD", "ADX"]].dropna().values
        if len(seq_data) < SEQ_LEN:
            logger.warning(f"[LSTM] Not enough data: {len(seq_data)} < {SEQ_LEN}")
            return None, position_type

        lstm_input = np.expand_dims(seq_data[-SEQ_LEN:], axis=0)
        pred = model.predict(lstm_input, verbose=0)[0][0]
        logger.info(f"[LSTM Prediction] {pred:.4f}")

        # Dynamic thresholds
        dynamic_up = 0.5 + min(last["ADX"] / 200, 0.05)  # Adjusted from /100 to /200, cap 0.05
        dynamic_down = 0.5 - min(last["ADX"] / 200, 0.05)
        logger.info(f"[Dynamic Thresholds] ADX: {last['ADX']:.2f}, Up: {dynamic_up:.3f}, Down: {dynamic_down:.3f}")

        logger.info(f"[Decision] Trend Up: {trend_up}, MACD Bullish: {macd_bullish}, RSI Strong: {rsi_strong}, "
                    f"Pred > Dynamic Up: {pred > dynamic_up}, Breakout: {breakout}")

        if trend_up and macd_bullish and rsi_strong and pred > dynamic_up and (breakout == "breakout_up" or breakout is None):
            logger.info("[Entry] BUY signal confirmed")
            return "buy", "buy"
        elif trend_down and pred < dynamic_down and (breakout == "breakout_down" or breakout is None):
            logger.info("[Entry] SELL signal confirmed ")
            return "sell", "sell"

        logger.info("[No Entry] No condition met for new position")
        return None, position_type

    except Exception as e:
        logger.exception(f"[Error] In signal generation: {e}")
        return None, position_type
