import backtrader as bt
import backtrader.indicators as btind
import pandas as pd
import numpy as np
import datetime
import logging
import os
import yaml
from pathlib import Path
import talib
from scipy.stats import norm

# === Create a logs folder if it doesn't exist ===
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)

# === Format filename with timestamp ===
log_filename = datetime.datetime.now().strftime("backtest_%Y%m%d_%H%M%S.log")
log_path = os.path.join(log_dir, log_filename)

# === Configure the logger ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s: %(message)s",
    handlers=[
        logging.FileHandler(log_path),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# === Load configuration ===
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
config_path = os.path.join(project_root, 'config', 'config.yaml')
if not os.path.exists(config_path):
    logger.error(f"Configuration file not found: {config_path}")
    raise FileNotFoundError(f"Configuration file not found: {config_path}")

with open(config_path, 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)

binance_config = config.get("binance", {})
SYMBOLS = binance_config.get("symbols", ['BTCUSDT', 'ETHUSDT', 'XRPUSDT'])
CAPITAL = binance_config.get("capital", 10000.0)
LEVERAGE = binance_config.get("leverage", 20.0)
TIMEFRAME = binance_config.get("timeframe", "5m")
TRAILING_UPDATE_INTERVAL = binance_config.get("trailing_update_interval", 10)
MAX_CONCURRENT_TRADES = binance_config.get("max_concurrent_trades", 10)
MAX_DRAWDOWN = 0.05 * CAPITAL

class UltraAgressiveTrailingStop:
    def __init__(self, symbol, trailing_distance_range=(0.002, 0.03), smoothing_alpha=0.3):
        self.symbol = symbol
        self.trailing_distance_range = trailing_distance_range
        self.smoothing_alpha = smoothing_alpha
        self.entry_price = 0.0
        self.current_stop_price = 0.0
        self.position_type = None
        self.quantity = 0.0
        self.trade_id = None
        self.active = False
        self.initial_stop_loss = 0.02
        self.smoothed_atr = None
        self.price_precision = 4 if symbol not in ['BTCUSDT', 'ETHUSDT'] else 2
        self.qty_precision = 2 if symbol not in ['BTCUSDT', 'ETHUSDT'] else 3
        self.price_tick = 0.0001

    def _update_smoothed_atr(self, new_atr):
        if self.smoothed_atr is None:
            self.smoothed_atr = new_atr
        else:
            self.smoothed_atr = (
                self.smoothing_alpha * new_atr + (1 - self.smoothing_alpha) * self.smoothed_atr
            )
        return self.smoothed_atr

    def _compute_trailing_distance(self, atr, adx, current_price):
        if atr is None or atr <= 0 or current_price <= 0:
            logger.warning(f"[{self.symbol}] ⚠️ Invalid ATR or price for trailing stop calculation.")
            return self.trailing_distance_range[0]
        
        atr_smooth = self._update_smoothed_atr(atr)
        spread = 0.0001
        base_trail = max((atr_smooth / current_price) * 1.5, spread / current_price + 0.5 * atr_smooth / current_price)
        adx_factor = max(0.2, min(1.0, 1.2 - (adx / 100)))
        trail = base_trail * adx_factor
        trail = max(self.trailing_distance_range[0], min(trail, self.trailing_distance_range[1]))
        logger.debug(
            f"[{self.symbol}] ATR={atr:.5f}, ATR_Smooth={atr_smooth:.5f}, ADX={adx:.2f}, "
            f"Spread={spread:.5f}, Base={base_trail:.5f}, ADX_Factor={adx_factor:.2f}, Final Trailing Distance={trail:.5f}"
        )
        return trail

    def initialize_trailing_stop(self, entry_price, position_type, quantity, atr, adx, trade_id):
        self.entry_price = float(entry_price)
        self.position_type = position_type
        self.quantity = float(quantity)
        self.trade_id = str(trade_id)
        self.active = True

        if self.entry_price <= 0:
            logger.error(f"[{self.symbol}] ❌ Invalid entry_price: {self.entry_price}")
            return None

        current_price = entry_price
        trailing_dist = self._compute_trailing_distance(atr, adx, current_price)
        initial_stop_dist = self.initial_stop_loss

        if self.position_type == 'long':
            stop_price = current_price * (1 - initial_stop_dist)
            self.current_stop_price = stop_price
        else:
            stop_price = current_price * (1 + initial_stop_dist)
            self.current_stop_price = stop_price

        logger.info(f"[{self.symbol}] ✅ Initial stop-loss placed at {stop_price:.2f} "
                    f"(Qty: {self.quantity:.4f}, trade_id: {self.trade_id}, trailing_dist: {trailing_dist:.6f})")
        return self.trade_id

    def update_trailing_stop(self, current_price, atr, adx):
        if not self.active or not self.entry_price:
            logger.error(f"[{self.symbol}] ❌ No active trailing stop or invalid entry_price")
            return

        current_price = float(current_price)
        if current_price <= 0:
            logger.error(f"[{self.symbol}] ❌ Invalid current_price: {current_price}")
            return

        trailing_dist = self._compute_trailing_distance(atr, adx, current_price)

        if self.position_type == 'long':
            new_stop = current_price * (1 - trailing_dist)
            should_update = new_stop > self.current_stop_price
        else:
            new_stop = current_price * (1 + trailing_dist)
            should_update = new_stop < self.current_stop_price

        if should_update:
            self.current_stop_price = new_stop
            logger.info(f"[{self.symbol}] 🔄 Trailing stop updated to {new_stop:.2f} (trade_id: {self.trade_id})")

    def verify_order_execution(self, current_price):
        if not self.active:
            return False

        current_price = float(current_price)
        if self.position_type == 'long' and current_price <= self.current_stop_price:
            exit_price = self.current_stop_price
            pnl = (exit_price - self.entry_price) * self.quantity
            logger.info(f"[{self.symbol}] ✅ Trailing stop executed at {exit_price:.2f}, PNL: {pnl:.4f}")
            self.close_position()
            return True
        elif self.position_type == 'short' and current_price >= self.current_stop_price:
            exit_price = self.current_stop_price
            pnl = (self.entry_price - exit_price) * self.quantity
            logger.info(f"[{self.symbol}] ✅ Trailing stop executed at {exit_price:.2f}, PNL: {pnl:.4f}")
            self.close_position()
            return True
        return False

    def close_position(self):
        if self.active:
            self.active = False
            self.current_stop_price = 0.0
            logger.info(f"[{self.symbol}] ✅ Position closed and trailing stop removed for trade_id: {self.trade_id}")

class TrailingStopManager:
    def __init__(self):
        self.stops = {}

    def has_trailing_stop(self, symbol):
        return symbol in self.stops and self.stops[symbol].active

    def initialize_trailing_stop(self, symbol, entry_price, position_type, quantity, atr, adx, trade_id):
        if symbol not in self.stops:
            self.stops[symbol] = UltraAgressiveTrailingStop(symbol)
        return self.stops[symbol].initialize_trailing_stop(entry_price, position_type, quantity, atr, adx, trade_id)

    def update_trailing_stop(self, symbol, current_price, atr, adx):
        if symbol in self.stops and self.stops[symbol].active:
            self.stops[symbol].update_trailing_stop(current_price, atr, adx)
            if self.stops[symbol].verify_order_execution(current_price):
                self.close_position(symbol)

    def close_position(self, symbol):
        if symbol in self.stops and self.stops[symbol].active:
            self.stops[symbol].close_position()
            del self.stops[symbol]
            logger.info(f"[{symbol}] ✅ Trailing stop manager closed position")

    def get_current_stop_price(self, symbol, trade_id=None):
        if symbol in self.stops and self.stops[symbol].active:
            return self.stops[symbol].current_stop_price
        return None

class MyTradingStrategy(bt.Strategy):
    params = (
        ('capital', CAPITAL),
        ('leverage', LEVERAGE),
        ('max_concurrent_trades', MAX_CONCURRENT_TRADES),
        ('trailing_distance_range', (0.002, 0.03)),
        ('smoothing_alpha', 0.3),
        ('adx_trend_threshold', 30),
        ('adx_range_threshold', 15),
        ('rsi_range_low', 40),
        ('rsi_range_high', 60),
        ('atr_range_threshold', 30),
        ('min_volume_threshold', 5),
        ('atr_period', 14),
        ('adx_period', 14),
    )

    def __init__(self):
        self.orders = {}
        self.active_positions = {}
        self.ts_manager = TrailingStopManager()
        self.atr = {}
        self.adx = {}
        self.rsi = {}
        self.macd = {}
        self.ema20 = {}
        self.ema50 = {}
        self.roc = {}
        self.last_signal = {}
        self.open_trades_count = 0
        self.current_positions = {symbol: None for symbol in SYMBOLS}
        self.last_order_details = {symbol: None for symbol in SYMBOLS}
        self.last_action_sent = {symbol: None for symbol in SYMBOLS}

        for data in self.datas:
            symbol = data._name
            self.orders[symbol] = None
            self.active_positions[symbol] = None
            self.atr[symbol] = btind.ATR(data, period=self.p.atr_period)
            self.adx[symbol] = btind.ADX(data, period=self.p.adx_period)
            self.rsi[symbol] = btind.RSI(data, period=14)
            self.macd[symbol] = btind.MACD(data, period_me1=12, period_me2=26, period_signal=9)
            self.ema20[symbol] = btind.EMA(data, period=20)
            self.ema50[symbol] = btind.EMA(data, period=50)
            self.roc[symbol] = btind.ROC(data, period=5)
            self.last_signal[symbol] = None

        logger.info("Strategy initialized for multi-asset backtest.")

    def log(self, txt, dt=None, symbol=None):
        dt = dt or self.datas[0].datetime.datetime(0)
        symbol = symbol or self.datas[0]._name
        logger.info(f'{dt.isoformat()} - {symbol} - {txt}')

    def notify_order(self, order):
        symbol = order.data._name
        if order.status in [order.Submitted, order.Accepted]:
            self.log(f"Order {order.getordername()} submitted/accepted for {symbol}", symbol=symbol)
            return

        if order.status == order.Completed:
            if order.isbuy():
                self.log(f'BUY EXECUTED, Price: {order.executed.price:.2f}, Cost: {order.executed.value:.2f}, Comm: {order.executed.comm:.2f}', symbol=symbol)
                self.active_positions[symbol] = 'long'
                self.current_positions[symbol] = {
                    'side': 'long',
                    'quantity': order.executed.size,
                    'price': order.executed.price,
                    'trade_id': order.ref
                }
                atr = self.atr[symbol][0]
                adx = self.adx[symbol][0]
                self.ts_manager.initialize_trailing_stop(
                    symbol=symbol,
                    entry_price=order.executed.price,
                    position_type='long',
                    quantity=order.executed.size,
                    atr=atr,
                    adx=adx,
                    trade_id=str(order.ref)
                )
            elif order.issell():
                self.log(f'SELL EXECUTED, Price: {order.executed.price:.2f}, Cost: {order.executed.value:.2f}, Comm: {order.executed.comm:.2f}', symbol=symbol)
                self.active_positions[symbol] = 'short'
                self.current_positions[symbol] = {
                    'side': 'short',
                    'quantity': abs(order.executed.size),
                    'price': order.executed.price,
                    'trade_id': order.ref
                }
                atr = self.atr[symbol][0]
                adx = self.adx[symbol][0]
                self.ts_manager.initialize_trailing_stop(
                    symbol=symbol,
                    entry_price=order.executed.price,
                    position_type='short',
                    quantity=abs(order.executed.size),
                    atr=atr,
                    adx=adx,
                    trade_id=str(order.ref)
                )

            if not self.getposition(data=order.data).size:
                self.log('CLOSURE ORDER EXECUTED. Position closed.', symbol=symbol)
                self.active_positions[symbol] = None
                self.current_positions[symbol] = None
                self.open_trades_count -= 1
                self.ts_manager.close_position(symbol)

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log(f'ORDER CANCELED/MARGIN/REJECTED: Type={order.getordername()}, Status={order.getstatusname()}', symbol=symbol)
            self.orders[symbol] = None

        self.orders[symbol] = None

    def notify_trade(self, trade):
        if trade.isclosed:
            self.log(f'TRADE CLOSED ({trade.data._name}): Gross PnL: {trade.pnl:.2f}, Net PnL: {trade.pnlcomm:.2f}', symbol=trade.data._name)

    def prepare_lstm_input(self, data):
        if len(data) < 100:
            return None
        df = pd.DataFrame({
            'close': data.close.get(size=100),
            'volume': data.volume.get(size=100),
            'RSI': self.rsi[data._name].get(size=100),
            'MACD': self.macd[data._name].macd.get(size=100),
            'ADX': self.adx[data._name].get(size=100)
        })
        data = df.values.astype(float)
        if np.isnan(data).any():
            df = df.ffill().bfill()
            data = np.nan_to_num(df.values.astype(float), nan=0.0, posinf=0.0, neginf=0.0)
        return data.reshape(1, 100, 5)

    def simulate_lstm_prediction(self, data):
        if len(data) < 100:
            return 0.5
        lstm_input = self.prepare_lstm_input(data)
        if lstm_input is None:
            return 0.5
        rsi_val = self.rsi[data._name][0]
        macd_val = self.macd[data._name].macd[0]
        macd_signal_val = self.macd[data._name].signal[0]
        adx_val = self.adx[data._name][0]
        prediction = 0.5
        if rsi_val > 50 and macd_val > macd_signal_val and adx_val > 15:
            prediction = 0.7
        elif rsi_val < 50 and macd_val < macd_signal_val and adx_val > 15:
            prediction = 0.3
        return prediction

    def calculate_dynamic_thresholds(self, adx, strategy_mode):
        base_up, base_down = {
            "trend": (0.60, 0.40),
            "scalp": (0.65, 0.35),
            "range": (0.55, 0.45)
        }.get(strategy_mode, (0.55, 0.45))
        adj = (adx / 1000)
        up = max(base_up - adj, base_down + 0.05)
        down = min(base_down + adj, up - 0.05)
        return round(up, 3), round(down, 3)

    def select_strategy_mode(self, adx, rsi, atr):
        if adx > self.p.adx_trend_threshold:
            return "trend"
        elif adx < self.p.adx_range_threshold and self.p.rsi_range_low < rsi < self.p.rsi_range_high and atr < self.p.atr_range_threshold:
            return "range"
        return "scalp"

    def log_indicator_summary(self, symbol, rsi, macd, adx, ema20, ema50, atr, roc, confidence_factors):
        table = f"""
        Indicator Summary for {symbol}:
        RSI: {rsi:.2f} ({'Near oversold' if rsi < 30 else 'Near overbought' if rsi > 70 else 'Neutral'})
        MACD: {macd:.4f} ({'Bullish' if macd > 0.5 else 'Bearish' if macd < -0.5 else 'Neutral'})
        ADX: {adx:.2f} ({'Strong trend' if adx >= 50 else 'Moderate trend' if adx >= 25 else 'Weak trend'})
        EMA20 vs EMA50: {ema20:.4f} {'>' if ema20 > ema50 else '<'} {ema50:.4f} ({'Bullish' if ema20 > ema50 else 'Bearish'})
        ROC: {roc:.2f}% ({'Strong bullish' if roc > 1 else 'Strong bearish' if roc < -1 else 'Neutral'})
        Confidence Factors: {', '.join(confidence_factors) if confidence_factors else 'None'}
        """
        self.log(table, symbol=symbol)

    def backtest_signal(self, data, action, threshold):
        try:
            returns = pd.Series(data.close.get(size=100)).pct_change().dropna()
            if len(returns) < 5:
                return False
            sharpe_ratio = returns.mean() / returns.std() * np.sqrt(252) if returns.std() != 0 else 0
            return sharpe_ratio > 0.3
        except Exception as e:
            self.log(f"Error in backtest_signal: {e}", symbol=data._name)
            return True

    def generate_trading_signal(self, data):
        symbol = data._name
        if len(data) < 100:
            self.log(f"Not enough data for {symbol}: {len(data)} rows", symbol=symbol)
            return "hold", None, 0.0, []

        current_price = data.close[0]
        rsi = self.rsi[symbol][0]
        macd = self.macd[symbol].macd[0]
        signal_line = self.macd[symbol].signal[0]
        adx = self.adx[symbol][0]
        ema20 = self.ema20[symbol][0]
        ema50 = self.ema50[symbol][0]
        atr = self.atr[symbol][0]
        roc = self.roc[symbol][0] * 100
        volume = data.volume[0]

        if volume < self.p.min_volume_threshold:
            self.log(f"Volume too low ({volume:.2f}) for {symbol}, signal ignored.", symbol=symbol)
            return "hold", None, 0.0, []

        strategy_mode = self.select_strategy_mode(adx, rsi, atr)
        self.log(f"Switched to {strategy_mode}, ADX: {adx:.2f}, RSI: {rsi:.2f}, ATR: {atr:.2f}, ROC: {roc:.2f}", symbol=symbol)

        prediction = self.simulate_lstm_prediction(data)
        self.log(f"LSTM prediction for {symbol}: {prediction:.4f}", symbol=symbol)

        dynamic_up, dynamic_down = self.calculate_dynamic_thresholds(adx, strategy_mode)
        self.log(f"Thresholds → Prediction: {prediction:.4f}, Dynamic Down: {dynamic_down:.4f}, Dynamic Up: {dynamic_up:.4f}", symbol=symbol)

        trend_up = ema20 > ema50
        trend_down = ema20 < ema50
        macd_bullish = macd > signal_line
        rsi_strong = (trend_up and rsi > 50) or (trend_down and rsi < 45) or (abs(rsi - 50) > 15)
        rolling_high = pd.Series(data.high.get(size=20)).max()
        rolling_low = pd.Series(data.low.get(size=20)).min()
        breakout_up = current_price > (rolling_high - 0.1 * atr)
        breakout_down = current_price < (rolling_low + 0.1 * atr)
        bullish_divergence = (data.close[0] < data.close[-3] and self.rsi[symbol][0] > self.rsi[symbol][-3])
        bearish_divergence = (data.close[0] > data.close[-3] and self.rsi[symbol][0] < self.rsi[symbol][-3])

        confidence_factors = []
        if prediction > 0.6 or prediction < 0.4:
            confidence_factors.append("LSTM strong")
        if (trend_up and macd > signal_line) or (trend_down and macd < signal_line):
            confidence_factors.append("MACD aligned")
        if rsi > 50 or rsi < 50:
            confidence_factors.append("RSI strong")
        if (trend_up and ema20 > ema50) or (trend_down and ema20 < ema50):
            confidence_factors.append("EMA trend")
        if abs(roc) > 0.3:
            confidence_factors.append("ROC momentum")
        if breakout_up or breakout_down:
            confidence_factors.append("Breakout detected")

        self.log_indicator_summary(symbol, rsi, macd, adx, ema20, ema50, atr, roc, confidence_factors)

        action = "hold"
        new_position = None
        signal_timestamp = int(data.datetime.datetime(0).timestamp() * 1000)

        expected_pnl = atr * 2 if atr > 1 else current_price * 0.01
        risk_reward_ratio = expected_pnl / atr if atr > 0 else 0
        self.log(f"Debug: expected_pnl={expected_pnl:.4f}, atr={atr:.4f}, risk_reward_ratio={risk_reward_ratio:.4f}", symbol=symbol)

        if len(confidence_factors) < 2 or risk_reward_ratio < 1.5:
            self.log(f"Signal rejected: Insufficient confidence ({len(confidence_factors)}/6) or risk/reward ratio ({risk_reward_ratio:.2f} < 1.5)", symbol=symbol)
            return "hold", None, 0.0, confidence_factors

        if action in ["buy", "sell"] and not self.backtest_signal(data, action, dynamic_up if action == 'buy' else dynamic_down):
            self.log(f"Signal rejected: Backtest failed for {action}", symbol=symbol)
            return "hold", None, 0.0, confidence_factors

        if strategy_mode == "scalp" and rsi_strong:
            if prediction > dynamic_up and roc > 0.3:
                action = "sell"
                new_position = "short"
            elif prediction < dynamic_down and roc < -0.3:
                action = "buy"
                new_position = "long"
        elif strategy_mode == "trend":
            if trend_up and macd_bullish and prediction > dynamic_up and roc > 0.3:
                action = "buy"
                new_position = "long"
            elif trend_down and not macd_bullish and prediction < dynamic_down and roc < -0.3:
                action = "sell"
                new_position = "short"
        elif strategy_mode == "range":
            if current_price <= (rolling_low + 0.1 * atr) and prediction > dynamic_up and roc > 0.3:
                action = "buy"
                new_position = "long"
            elif current_price >= (rolling_high - 0.1 * atr) and prediction < dynamic_down and roc < -0.3:
                action = "sell"
                new_position = "short"

        if bullish_divergence and prediction > 0.65 and roc > 0.3:
            action = "buy"
            new_position = "long"
        elif bearish_divergence and prediction < 0.35 and roc < -0.3:
            action = "sell"
            new_position = "short"

        current_position = self.current_positions.get(symbol)
        if current_position == "long" and (trend_down or not macd_bullish or roc < -0.3):
            action = "close_buy"
            new_position = None
        elif current_position == "short" and (trend_up or macd_bullish or roc > 0.3):
            action = "close_sell"
            new_position = None
        elif current_position is None and action in ["buy", "sell"]:
            quantity = (self.p.capital * self.p.leverage) / current_price if current_price > 0 else 0.0
            self.log(f"Order qty: {quantity:.4f}, Margin required: {(quantity * current_price / self.p.leverage):.2f}, Available cash: {self.broker.getcash():.2f}", symbol=symbol)
            new_position = {
                "side": "long" if action == "buy" else "short",
                "quantity": quantity,
                "price": current_price,
                "trade_id": str(signal_timestamp)
            }

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
            self.log(f"Action generated: {action}, Confidence: {len(confidence_factors)}/6, Prediction: {prediction:.4f}, Risk/Reward: {risk_reward_ratio:.2f}", symbol=symbol)

        if self.last_action_sent.get(symbol) and isinstance(self.last_action_sent[symbol], tuple) and action == self.last_action_sent[symbol][0]:
            self.log(f"Signal {action} ignored as it was sent previously.", symbol=symbol)
            return "hold", None, 0.0, []

        if action != "hold":
            self.last_action_sent[symbol] = (action, signal_timestamp)

        return action, new_position, confidence, confidence_factors

    def next(self):
        for data in self.datas:
            symbol = data._name
            current_price = data.close[0]
            position = self.getposition(data=data)
            self.log(f"Broker cash: {self.broker.getcash():.2f}, Portfolio value: {self.broker.getvalue():.2f}, Open trades: {self.open_trades_count}/{self.p.max_concurrent_trades}", symbol=symbol)

            if self.open_trades_count >= self.p.max_concurrent_trades and not position.size:
                self.log(f"Max concurrent trades ({self.open_trades_count}/{self.p.max_concurrent_trades}) reached, skipping {symbol}", symbol=symbol)
                continue

            action, new_position, confidence, confidence_factors = self.generate_trading_signal(data)

            if action == "hold" or confidence < 0.5:
                self.ts_manager.update_trailing_stop(symbol, current_price, self.atr[symbol][0], self.adx[symbol][0])
                continue

            if action in ["buy", "sell"] and not position.size:
                qty = (self.p.capital * self.p.leverage) / current_price
                qty = max(qty, 0.001)
                margin_required = qty * current_price / self.p.leverage
                if self.broker.getcash() < margin_required:
                    self.log(f"Insufficient margin for {symbol}, Required: {margin_required:.2f}, Available: {self.broker.getcash():.2f}", symbol=symbol)
                    continue

                if action == "buy":
                    order = self.buy(data=data, size=qty)
                else:
                    order = self.sell(data=data, size=qty)
                self.orders[symbol] = order
                self.open_trades_count += 1
                self.log(f"Placed {action} order for {symbol}, Qty: {qty:.4f}, Price: {current_price:.2f}", symbol=symbol)

            elif action in ["close_buy", "close_sell"] and position.size:
                self.close(data=data)
                self.log(f"Closing position for {symbol}", symbol=symbol)

            self.ts_manager.update_trailing_stop(symbol, current_price, self.atr[symbol][0], self.adx[symbol][0])

    def stop(self):
        for data in self.datas:
            symbol = data._name
            if self.getposition(data=data).size:
                self.close(data=data)
                self.log(f"Closed position for {symbol} at end of backtest", symbol=symbol)
        self.log(f"Final broker cash: {self.broker.getcash():.2f}, Final portfolio value: {self.broker.getvalue():.2f}")

def run_backtest(symbols_data):
    cerebro = bt.Cerebro()
    cerebro.addstrategy(MyTradingStrategy)

    for symbol, data_path in symbols_data.items():
        try:
            df = pd.read_csv(data_path)
            required_columns = ['datetime', 'open', 'high', 'low', 'close', 'volume']
            if not all(col in df.columns for col in required_columns):
                raise ValueError(f"The CSV file for {symbol} must contain the columns: {required_columns}")
            df['datetime'] = pd.to_datetime(df['datetime'])
            df = df.sort_values('datetime').ffill().bfill()
            df.to_csv(f'./backtest/data_cleaned_{symbol}.csv', index=False)
            
            data = bt.feeds.PandasData(
                dataname=df,
                datetime='datetime',
                open='open',
                high='high',
                low='low',
                close='close',
                volume='volume',
                timeframe=bt.TimeFrame.Minutes,
                compression=5,
                name=symbol
            )
            cerebro.adddata(data)
            logger.info(f"Data loaded for {symbol} from {data_path}")
        except Exception as e:
            logger.error(f"Error loading data for {symbol}: {e}")
            continue

    cerebro.broker.setcash(CAPITAL)
    cerebro.broker.setcommission(commission=0.0004)
    cerebro.broker.set_slippage_perc(perc=0.0001)
    logger.info(f'Broker cash set to: {cerebro.broker.getcash():.2f}')

    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe', timeframe=bt.TimeFrame.Days)
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='trades')
    cerebro.addanalyzer(bt.analyzers.SQN, _name='sqn')

    cerebro.addindicator(btind.RSI, period=14)
    cerebro.addindicator(btind.MACD)
    cerebro.addindicator(btind.EMA, period=20)
    cerebro.addindicator(btind.EMA, period=50)

    logger.info(f'Initial capital: {cerebro.broker.getvalue():.2f}')
    results = cerebro.run()
    strategy = results[0]

    final_value = cerebro.broker.getvalue()
    logger.info(f'Final capital: {final_value:.2f}')
    logger.info(f'Profit: {final_value - CAPITAL:.2f}')

    if 'sharpe' in strategy.analyzers:
        sharpe_ratio = strategy.analyzers.sharpe.get_analysis()
        logger.info(f'Sharpe Ratio: {sharpe_ratio.get("sharperatio", "N/A") or "N/A":.2f}')
    if 'drawdown' in strategy.analyzers:
        drawdown_analysis = strategy.analyzers.drawdown.get_analysis()
        logger.info(f'Max Drawdown: {drawdown_analysis.get("max", {}).get("drawdown", 0.0):.2f}%')
        logger.info(f'Longest Drawdown Duration: {drawdown_analysis.get("max", {}).get("len", 0)} bars')
    if 'trades' in strategy.analyzers:
        trades_analysis = strategy.analyzers.trades.get_analysis()
        logger.info(f'Total Trades: {trades_analysis.total.opened}')
        logger.info(f'Winning Trades: {trades_analysis.won.total}')
        logger.info(f'Losing Trades: {trades_analysis.lost.total}')
        if trades_analysis.won.total > 0:
            logger.info(f'Avg Win: {trades_analysis.won.pnl.average:.2f}')
        if trades_analysis.lost.total > 0:
            logger.info(f'Avg Loss: {trades_analysis.lost.pnl.average:.2f}')
        if trades_analysis.won.total > 0 and trades_analysis.lost.total > 0:
            logger.info(f'Win Rate: {(trades_analysis.won.total / trades_analysis.total.opened * 100):.2f}%')
            logger.info(f'Avg Win/Avg Loss Ratio: {(trades_analysis.won.pnl.average / abs(trades_analysis.lost.pnl.average)):.2f}')
    if 'sqn' in strategy.analyzers:
        sqn_value = strategy.analyzers.sqn.get_analysis()
        logger.info(f'System Quality Number (SQN): {sqn_value.get("sqn", "N/A"):.2f}')

    try:
        logger.info("Generating performance chart...")
        cerebro.plot(style='candlestick', iplot=False, numfigs=1, savefig=True, figpath='backtest_result.png')
        logger.info("Chart saved as 'backtest_result.png'.")
    except Exception as e:
        logger.error(f"Error generating chart: {e}")

if __name__ == '__main__':
    symbols_data = {
        'BTCUSDT': './backtest/BTCUSDT.csv',
        'ETHUSDT': './backtest/ETHUSDT.csv',
        'XRPUSDT': './backtest/XRPUSDT.csv',
    }
    run_backtest(symbols_data)