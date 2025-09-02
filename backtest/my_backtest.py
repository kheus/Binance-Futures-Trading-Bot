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
from tensorflow import keras
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import GRU, Dense, Bidirectional  # Changed to GRU for better performance
from sklearn.preprocessing import MinMaxScaler
from tensorflow.keras.callbacks import EarlyStopping, ReduceLROnPlateau
from tensorflow.keras import Input
from binance.client import Client  # Added for data download

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
CAPITAL = binance_config.get("capital", 100.0)
LEVERAGE = binance_config.get("leverage", 5.0)  # Reduced from 20.0 to mitigate risk
TIMEFRAME = binance_config.get("timeframe", "5m")
TRAILING_UPDATE_INTERVAL = binance_config.get("trailing_update_interval", 10)
MAX_CONCURRENT_TRADES = binance_config.get("max_concurrent_trades", 10)
MAX_DRAWDOWN = 0.05 * CAPITAL

# Added: Function to download historical data from Binance if CSV not present
def download_binance_data(symbol, interval='5m', start_time=None, end_time=None, api_key='', api_secret=''):
    if not api_key or not api_secret:
        logger.error("Binance API key and secret required for download.")
        return None
    
    client = Client(api_key, api_secret)
    if start_time is None:
        start_time = datetime.datetime.now() - pd.DateOffset(months=3)  # 3 months back
    if end_time is None:
        end_time = datetime.datetime.now()
    
    klines = client.get_historical_klines(
        symbol=symbol,
        interval=interval,
        start_str=str(start_time),
        end_str=str(end_time)
    )
    
    if not klines:
        logger.warning(f"No data downloaded for {symbol}.")
        return None
    
    df = pd.DataFrame(klines, columns=['datetime', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_asset_volume', 'num_trades', 'taker_buy_base', 'taker_buy_quote', 'ignore'])
    df['datetime'] = pd.to_datetime(df['datetime'], unit='ms')
    df = df[['datetime', 'open', 'high', 'low', 'close', 'volume']].astype(float, errors='ignore')
    return df

class UltraAgressiveTrailingStop:
    def __init__(self, symbol, smoothing_alpha=0.3, min_profit_activate=0.02, initial_sl_multiplier=2.0, min_sl_pct=0.005):  # Relaxed min_profit to 2%, min_sl to 0.5%
        self.symbol = symbol
        self.smoothing_alpha = smoothing_alpha
        self.min_profit_activate = min_profit_activate
        self.initial_sl_multiplier = initial_sl_multiplier
        self.min_sl_pct = min_sl_pct
        self.profit_lock_margin = 0.05  # Reduced to 5% for better profit locking
        self.entry_price = 0.0
        self.current_stop_price = 0.0
        self.position_type = None
        self.quantity = 0.0
        self.trade_id = None
        self.active = False
        self.atr_smooth = None
        self.price_precision = 4 if symbol not in ['BTCUSDT', 'ETHUSDT'] else 2
        self.qty_precision = 2 if symbol not in ['BTCUSDT', 'ETHUSDT'] else 3
        self.price_tick = 0.0001
        if symbol in ['BTCUSDT', 'ETHUSDT']:
            self.min_trail = 0.005  # 0.5%
            self.max_trail = 0.05   # 5%
        else:
            self.min_trail = 0.008
            self.max_trail = 0.08

    def _update_smoothed_atr(self, new_atr):
        if self.atr_smooth is None:
            self.atr_smooth = new_atr
        else:
            self.atr_smooth = self.smoothing_alpha * new_atr + (1 - self.smoothing_alpha) * self.atr_smooth
        return self.atr_smooth

    def _compute_trailing_distance(self, atr, adx, current_price, spread):
        if atr is None or atr <= 0 or current_price <= 0:
            logger.warning(f"[{self.symbol}] ⚠️ Invalid ATR or price.")
            return self.min_trail
        
        atr_smooth = self._update_smoothed_atr(atr)
        base_trail = max((atr_smooth / current_price) * 1.5, spread / current_price + 0.5 * atr_smooth / current_price)
        adx_factor = max(0.5, min(1.0, 1.0 - (adx / 200)))
        trail = max(self.min_trail, min(base_trail * adx_factor, self.max_trail))
        logger.debug(f"[{self.symbol}] ATR={atr:.5f}, Smooth={atr_smooth:.5f}, ADX={adx:.2f}, Trail={trail:.5f}")
        return trail

    def initialize_trailing_stop(self, entry_price, position_type, quantity, atr, adx, trade_id):
        self.entry_price = float(entry_price)
        self.position_type = position_type.lower()
        self.quantity = float(quantity)
        self.trade_id = str(trade_id)
        self.active = True

        if self.entry_price <= 0:
            logger.error(f"[{self.symbol}] Invalid entry_price.")
            return None

        trail_distance = max(self.initial_sl_multiplier * atr / self.entry_price, self.min_sl_pct)
        if self.position_type == "long":
            self.current_stop_price = self.entry_price * (1 - trail_distance)
        else:
            self.current_stop_price = self.entry_price * (1 + trail_distance)

        self.current_stop_price = round(self.current_stop_price, self.price_precision)
        logger.info(f"[{self.symbol}] Initial SL at {self.current_stop_price:.2f} (Qty: {self.quantity:.4f})")
        return self.trade_id

    def update_trailing_stop(self, current_price, atr, adx, spread=0.001):  # Increased spread for realism
        if not self.active or not self.entry_price:
            return

        current_price = float(current_price)
        if current_price <= 0:
            return

        profit_pct = ((current_price - self.entry_price) / self.entry_price if self.position_type == "long" else (self.entry_price - current_price) / self.entry_price)
        if profit_pct < self.min_profit_activate:
            return

        trailing_dist = self._compute_trailing_distance(atr, adx, current_price, spread)

        if self.position_type == 'long':
            new_stop = max(self.current_stop_price, current_price * (1 - trailing_dist))
            new_stop = max(new_stop, self.entry_price * (1 + self.profit_lock_margin))
            should_update = new_stop >= self.current_stop_price + self.price_tick
        else:
            new_stop = min(self.current_stop_price, current_price * (1 + trailing_dist))
            new_stop = min(new_stop, self.entry_price * (1 - self.profit_lock_margin))
            should_update = new_stop <= self.current_stop_price - self.price_tick

        if should_update:
            self.current_stop_price = round(new_stop, self.price_precision)
            logger.info(f"[{self.symbol}] Trailing updated to {self.current_stop_price:.2f}")

    def verify_order_execution(self, current_price):
        if not self.active:
            return False

        current_price = float(current_price)
        if self.position_type == 'long' and current_price <= self.current_stop_price:
            exit_price = self.current_stop_price
            pnl = (exit_price - self.entry_price) * self.quantity
            logger.info(f"[{self.symbol}] Stop executed at {exit_price:.2f}, PNL: {pnl:.4f}")
            self.close_position()
            return True
        elif self.position_type == 'short' and current_price >= self.current_stop_price:
            exit_price = self.current_stop_price
            pnl = (self.entry_price - exit_price) * self.quantity
            logger.info(f"[{self.symbol}] Stop executed at {exit_price:.2f}, PNL: {pnl:.4f}")
            self.close_position()
            return True
        return False

    def close_position(self):
        if self.active:
            self.active = False
            self.current_stop_price = 0.0
            logger.info(f"[{self.symbol}] Position closed for {self.trade_id}")

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
            logger.info(f"[{symbol}] Manager closed position")

    def get_current_stop_price(self, symbol, trade_id=None):
        if symbol in self.stops and self.stops[symbol].active:
            return self.stops[symbol].current_stop_price
        return None

class MyTradingStrategy(bt.Strategy):
    params = (
        ('capital', CAPITAL),
        ('leverage', LEVERAGE),
        ('max_concurrent_trades', MAX_CONCURRENT_TRADES),
        ('lstm_models', None),
        ('lstm_scalers', None),
    )

    def __init__(self):
        self.orders = {}
        self.current_positions = {}
        self.active_positions = None  # Consolidated
        self.open_trades_count = 0
        self.ts_manager = TrailingStopManager()
        self.atr = {}
        self.adx = {}
        self.rsi = {}
        self.macd = {}
        self.ema20 = {}
        self.ema50 = {}
        self.roc = {}
        self.last_signal_time = {}

        for data in self.datas:
            symbol = data._name
            self.atr[symbol] = btind.ATR(data, period=14)
            self.adx[symbol] = btind.ADX(data, period=14)
            self.rsi[symbol] = btind.RSI(data, period=14)
            self.macd[symbol] = btind.MACD(data)
            self.ema20[symbol] = btind.EMA(data, period=20)
            self.ema50[symbol] = btind.EMA(data, period=50)
            self.roc[symbol] = btind.ROC(data, period=5)
            self.last_signal_time[symbol] = None

        logger.info("Strategy initialized for multi-asset backtest.")

    def log(self, txt, symbol=None, dt=None):
        dt = dt or self.datas[0].datetime.datetime(0)
        logger.info(f"{dt.isoformat()} - {symbol} - {txt}" if symbol else f"{dt.isoformat()} - {txt}")

    def prepare_lstm_input(self, symbol, data):
        if len(data) < 60:  # Aligned to seq_len
            return None
        
        df = pd.DataFrame({
            'close': data.close.get(size=60),
            'volume': data.volume.get(size=60),
            'RSI': self.rsi[symbol].get(size=60),
            'MACD': self.macd[symbol].macd.get(size=60),
            'ADX': self.adx[symbol].get(size=60)
        })
        df = df.fillna(0)  # Better handling
        scaler = self.p.lstm_scalers.get(symbol)
        if scaler:
            scaled = scaler.transform(df.values)
            return scaled.reshape(1, 60, 5)
        return None

    def predict_with_lstm(self, symbol, data):
        input_data = self.prepare_lstm_input(symbol, data)
        if input_data is None:
            return 0.5
        
        model = self.p.lstm_models.get(symbol)
        if model:
            return model.predict(input_data)[0][0]
        return 0.5

    def generate_trading_signal(self, symbol, data):
        if len(data) < 100 or data.volume[0] < 5:
            return 'hold'

        current_price = data.close[0]
        atr = max(self.atr[symbol][0], current_price * 0.005)  # Min ATR 0.5%
        adx = self.adx[symbol][0]
        rsi = self.rsi[symbol][0]
        macd = self.macd[symbol][0]
        macd_signal = self.macd[symbol].signal[0]
        ema20 = self.ema20[symbol][0]
        ema50 = self.ema50[symbol][0]
        roc = self.roc[symbol][0]
        lstm_pred = self.predict_with_lstm(symbol, data)

        # Mode selection
        if adx > 25:  # Lowered from 30 for more trend mode
            mode = 'trend'
        elif adx < 15 and 40 < rsi < 60 and atr < 30:
            mode = 'range'
        else:
            mode = 'scalp'

        logger.info(f"[{symbol}] Switched to {mode}, ADX: {adx:.2f}, RSI: {rsi:.2f}, ATR: {atr:.2f}, ROC: {roc:.2f}")

        # Dynamic thresholds
        adx_adjust = max(0.1, min(0.3, adx / 100))
        if mode == 'trend':
            upper_threshold = 0.6 - adx_adjust
            lower_threshold = 0.4 + adx_adjust
        elif mode == 'range':
            upper_threshold = 0.55 - adx_adjust
            lower_threshold = 0.45 + adx_adjust
        else:
            upper_threshold = 0.55 - adx_adjust
            lower_threshold = 0.45 + adx_adjust

        logger.info(f"[{symbol}] Thresholds → Prediction: {lstm_pred:.4f}, Down: {lower_threshold:.4f}, Up: {upper_threshold:.4f}")

        # Confidence factors
        confidence_factors = []
        if lstm_pred > 0.6 or lstm_pred < 0.4:
            confidence_factors.append('LSTM strong')
        if (macd > macd_signal and macd > 0) or (macd < macd_signal and macd < 0):
            confidence_factors.append('MACD aligned')
        if rsi < 30 or rsi > 70:
            confidence_factors.append('RSI extreme')
        if current_price > max(data.high.get(size=20)) or current_price < min(data.low.get(size=20)):
            confidence_factors.append('Breakout detected')

        logger.info(f"[{symbol}] Indicator Summary: RSI: {rsi:.2f} ({'Oversold' if rsi < 30 else 'Overbought' if rsi > 70 else 'Neutral'}), MACD: {macd:.4f} ({'Bullish' if macd > 0 else 'Bearish'}), ADX: {adx:.2f} ({'Strong' if adx > 30 else 'Moderate' if adx > 20 else 'Weak'}), EMA20 vs EMA50: {ema20:.4f} {'>' if ema20 > ema50 else '<'} {ema50:.4f} ({'Bullish' if ema20 > ema50 else 'Bearish'}), ROC: {roc:.2f}% ({'Positive' if roc > 0 else 'Negative'}), Confidence: {', '.join(confidence_factors) or 'None'}")

        # Risk/reward
        expected_reward = 2 * atr
        risk = atr
        risk_reward_ratio = expected_reward / risk if risk > 0 else 0
        logger.info(f"[{symbol}] Debug: expected_pnl={expected_reward:.4f}, atr={atr:.4f}, ratio={risk_reward_ratio:.4f}")

        if len(confidence_factors) < 1 or risk_reward_ratio < 1.2:  # Relaxed to >=1 factor
            logger.info(f"[{symbol}] Signal rejected: Confidence ({len(confidence_factors)}/4) or ratio ({risk_reward_ratio:.2f} < 1.2)")
            return 'hold'

        # Signals per mode
        if mode == 'trend':
            if lstm_pred > upper_threshold and ema20 > ema50 and macd > macd_signal:
                return 'buy'
            elif lstm_pred < lower_threshold and ema20 < ema50 and macd < macd_signal:
                return 'sell'
        elif mode == 'range':
            rolling_min = min(data.low.get(size=20))
            rolling_max = max(data.high.get(size=20))
            if current_price < rolling_min + 0.1 * atr and lstm_pred > upper_threshold:
                return 'buy'
            elif current_price > rolling_max - 0.1 * atr and lstm_pred < lower_threshold:
                return 'sell'
        else:  # scalp, fixed to momentum (swapped from contrarian)
            if lstm_pred > upper_threshold and roc > 0.005:
                return 'buy'
            elif lstm_pred < lower_threshold and roc < -0.005:
                return 'sell'

        # Close logic (fixed bug)
        current_position = self.current_positions.get(symbol)
        if current_position and current_position.get('side') == 'long':
            if ema20 < ema50 or roc < -0.01:
                return 'close_buy'
        elif current_position and current_position.get('side') == 'short':
            if ema20 > ema50 or roc > 0.01:
                return 'close_sell'

        return 'hold'

    def next(self):
        for data in self.datas:
            symbol = data._name
            position = self.getposition(data).size
            current_price = data.close[0]

            self.log(f"Broker cash: {self.broker.getcash():.2f}, Portfolio value: {self.broker.getvalue():.2f}, Open trades: {self.open_trades_count}/{self.p.max_concurrent_trades}")

            if self.last_signal_time.get(symbol) == data.datetime.datetime(0):
                continue

            action = self.generate_trading_signal(symbol, data)
            self.last_signal_time[symbol] = data.datetime.datetime(0)

            if self.open_trades_count >= self.p.max_concurrent_trades:
                continue

            if self.ts_manager.has_trailing_stop(symbol):
                self.ts_manager.update_trailing_stop(symbol, current_price, self.atr[symbol][0], self.adx[symbol][0])
                continue

            if action in ["buy", "sell"] and not position:
                qty = (self.p.capital * self.p.leverage) / current_price
                qty = max(qty, 0.001)
                margin_required = qty * current_price / self.p.leverage
                if self.broker.getcash() < margin_required:
                    self.log(f"Insufficient margin for {symbol}", symbol=symbol)
                    continue

                if action == "buy":
                    order = self.buy(data=data, size=qty)
                    side = 'long'
                else:
                    order = self.sell(data=data, size=qty)
                    side = 'short'
                self.orders[symbol] = order
                self.open_trades_count += 1
                self.current_positions[symbol] = {'side': side, 'quantity': qty}
                self.log(f"Placed {action} order for {symbol}, Qty: {qty:.4f}, Price: {current_price:.2f}", symbol=symbol)

                # Initialize trailing stop
                trade_id = str(datetime.datetime.now())
                self.ts_manager.initialize_trailing_stop(symbol, current_price, side, qty, self.atr[symbol][0], self.adx[symbol][0], trade_id)

            elif action in ["close_buy", "close_sell"] and position:
                self.close(data=data)
                self.ts_manager.close_position(symbol)
                self.open_trades_count -= 1
                if symbol in self.current_positions:
                    del self.current_positions[symbol]
                self.log(f"Closing position for {symbol}", symbol=symbol)

            self.ts_manager.update_trailing_stop(symbol, current_price, self.atr[symbol][0], self.adx[symbol][0])

    def stop(self):
        for data in self.datas:
            symbol = data._name
            if self.getposition(data).size:
                self.close(data=data)
                self.ts_manager.close_position(symbol)
                self.log(f"Closed position for {symbol} at end", symbol=symbol)
        self.log(f"Final cash: {self.broker.getcash():.2f}, Value: {self.broker.getvalue():.2f}")

def create_sequences(X, y, seq_len=60):
    Xs, ys = [], []
    for i in range(len(X) - seq_len):
        Xs.append(X[i:i+seq_len])
        ys.append(y[i+seq_len])
    return np.array(Xs), np.array(ys)

def create_gru_model(input_shape):  # Changed to GRU for better performance in crypto prediction
    model = Sequential()
    model.add(Input(shape=input_shape))
    model.add(GRU(100, return_sequences=True, dropout=0.2))
    model.add(GRU(100, dropout=0.2))
    model.add(Dense(1, activation='sigmoid'))  # Kept binary for up/down
    model.compile(optimizer=keras.optimizers.Adam(learning_rate=0.0001), loss='binary_crossentropy')
    return model

def train_lstm_model(symbols_data, epochs=50, batch_size=32, seq_len=60):  # Increased epochs/batch for better training
    models = {}
    scalers = {}

    for symbol, data_path in symbols_data.items():
        logger.info(f"Training GRU for {symbol} using {data_path}")
        df = pd.read_csv(data_path)
        df['datetime'] = pd.to_datetime(df['datetime'])
        df = df.sort_values('datetime')

        three_months_ago = datetime.datetime.now() - pd.DateOffset(months=3)
        df = df[df['datetime'] >= three_months_ago]
        if df.empty:
            continue

        df['RSI'] = talib.RSI(df['close'], timeperiod=14)
        macd, macd_signal, _ = talib.MACD(df['close'])
        df['MACD'] = macd
        df['ADX'] = talib.ADX(df['high'], df['low'], df['close'], timeperiod=14)

        df = df.dropna().reset_index(drop=True)

        features = df[['close', 'volume', 'RSI', 'MACD', 'ADX']].values
        labels = (df['close'].shift(-1) > df['close']).astype(int).values[:-1]
        features = features[:-1]

        split = int(len(features) * 0.8)
        X_train, X_val = features[:split], features[split:]
        y_train, y_val = labels[:split], labels[split:]

        scaler = MinMaxScaler()
        X_train_scaled = scaler.fit_transform(X_train)
        X_val_scaled = scaler.transform(X_val)

        X_seq, y_seq = create_sequences(X_train_scaled, y_train, seq_len)
        X_val_seq, y_val_seq = create_sequences(X_val_scaled, y_val, seq_len)

        model = create_gru_model((X_seq.shape[1], X_seq.shape[2]))

        callbacks = [
            EarlyStopping(monitor='val_loss', patience=10, restore_best_weights=True),
            ReduceLROnPlateau(monitor='val_loss', factor=0.5, patience=5, min_lr=1e-6)
        ]

        history = model.fit(
            X_seq, y_seq,
            validation_data=(X_val_seq, y_val_seq),
            epochs=epochs,
            batch_size=batch_size,
            callbacks=callbacks,
            verbose=1
        )

        models[symbol] = model
        scalers[symbol] = scaler
        logger.info(f"Finished training GRU for {symbol}")

    return models, scalers

def run_backtest(symbols_data, api_key='', api_secret=''):
    # Download data if not present
    for symbol, path in symbols_data.items():
        if not os.path.exists(path):
            logger.info(f"Downloading data for {symbol}")
            df = download_binance_data(symbol, interval='5m', api_key=api_key, api_secret=api_secret)
            if df is not None:
                df.to_csv(path, index=False)
            else:
                continue

    lstm_models, lstm_scalers = train_lstm_model(symbols_data)

    cerebro = bt.Cerebro()
    cerebro.addstrategy(MyTradingStrategy, lstm_models=lstm_models, lstm_scalers=lstm_scalers)

    for symbol, data_path in symbols_data.items():
        try:
            df = pd.read_csv(data_path)
            required_columns = ['datetime', 'open', 'high', 'low', 'close', 'volume']
            if not all(col in df.columns for col in required_columns):
                raise ValueError(f"Missing columns in {symbol} CSV")

            df['datetime'] = pd.to_datetime(df['datetime'])
            df = df.sort_values('datetime').ffill().bfill()

            one_month_ago = datetime.datetime.now() - pd.DateOffset(months=1)
            df = df[df['datetime'] >= one_month_ago]
            if df.empty:
                continue

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
            logger.info(f"Data loaded for {symbol}")
        except Exception as e:
            logger.error(f"Error for {symbol}: {e}")
            continue

    cerebro.broker.setcash(CAPITAL)
    cerebro.broker.setcommission(commission=0.0004)  # 0.04%
    cerebro.broker.set_slippage_perc(perc=0.001)  # Increased to 0.1% for realism
    logger.info(f"Broker cash: {cerebro.broker.getcash():.2f}")

    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe', timeframe=bt.TimeFrame.Days)
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='trades')
    cerebro.addanalyzer(bt.analyzers.SQN, _name='sqn')

    cerebro.addindicator(btind.RSI, period=14)
    cerebro.addindicator(btind.MACD)
    cerebro.addindicator(btind.EMA, period=20)
    cerebro.addindicator(btind.EMA, period=50)

    logger.info(f"Initial value: {cerebro.broker.getvalue():.2f}")
    results = cerebro.run()
    strategy = results[0]

    final_value = cerebro.broker.getvalue()
    logger.info(f"Final value: {final_value:.2f}")
    logger.info(f"Profit: {final_value - CAPITAL:.2f}")

    if 'sharpe' in strategy.analyzers:
        sharpe = strategy.analyzers.sharpe.get_analysis().get('sharperatio', 'N/A')
        logger.info(f"Sharpe: {sharpe:.2f}")
    if 'drawdown' in strategy.analyzers:
        dd = strategy.analyzers.drawdown.get_analysis()
        logger.info(f"Max DD: {dd['max']['drawdown']:.2f}%")
        logger.info(f"DD Duration: {dd['max']['len']}")
    if 'trades' in strategy.analyzers:
        ta = strategy.analyzers.trades.get_analysis()
        logger.info(f"Total Trades: {ta['total']['total']}")
        logger.info(f"Wins: {ta['won']['total']}")
        logger.info(f"Losses: {ta['lost']['total']}")
        if ta['won']['total'] > 0:
            logger.info(f"Avg Win: {ta['won']['pnl']['average']:.2f}")
        if ta['lost']['total'] > 0:
            logger.info(f"Avg Loss: {ta['lost']['pnl']['average']:.2f}")
        if ta['total']['total'] > 0:
            logger.info(f"Win Rate: {(ta['won']['total'] / ta['total']['total'] * 100):.2f}%")
    if 'sqn' in strategy.analyzers:
        sqn = strategy.analyzers.sqn.get_analysis().get('sqn', 'N/A')
        logger.info(f"SQN: {sqn:.2f}")

    try:
        logger.info("Generating chart...")
        cerebro.plot(style='candlestick', iplot=False, numfigs=1, savefig=True, figpath='backtest_result.png')
        logger.info("Chart saved.")
    except Exception as e:
        logger.error(f"Chart error: {e}")

if __name__ == '__main__':
    # Add your Binance API keys here
    API_KEY = 'tjmxSoMLy22RZEWO7sTbmMPa6t2wEHrYZElLAKep8VlAF1SbGlpVm9OIWGTIuJl6'
    API_SECRET = 'Z3hF9Tf83pP2E0KCx31n61hWxEdCTsMgNmVj35qzX68uaSyvXTo2wqmWBA8StR3N'

    symbols_data = {
        'BTCUSDT': './backtest/BTCUSDT_month.csv',
        'ETHUSDT': './backtest/ETHUSDT_month.csv',
        'XRPUSDT': './backtest/XRPUSDT_month.csv',
    }
    run_backtest(symbols_data, api_key=API_KEY, api_secret=API_SECRET)