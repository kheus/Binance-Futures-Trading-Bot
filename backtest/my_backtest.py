# backtest/my_backtest.py
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
from tensorflow.keras.layers import GRU, Dense, Bidirectional
from sklearn.preprocessing import MinMaxScaler
from tensorflow.keras.callbacks import EarlyStopping, ReduceLROnPlateau
from tensorflow.keras import Input
from binance.client import Client

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
CAPITAL = binance_config.get("capital", 100.0)  # Force to 100 if needed
LEVERAGE = binance_config.get("leverage", 20.0)
TIMEFRAME = binance_config.get("timeframe", "1h")
TRAILING_UPDATE_INTERVAL = binance_config.get("trailing_update_interval", 10)
MAX_CONCURRENT_TRADES = binance_config.get("max_concurrent_trades", 1)
MAX_DRAWDOWN = 0.05 * CAPITAL

# Function to download historical data from Binance if CSV not present
def download_binance_data(symbol, interval='1h', start_time=None, end_time=None, api_key='', api_secret=''):
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
    logger.info(f"Downloaded {len(df)} rows for {symbol} from {df['datetime'].min()} to {df['datetime'].max()}")
    return df

class UltraAgressiveTrailingStop:
    def __init__(self, symbol, fixed_sl_pct=0.05, take_profit_pct=0.08, trail_multiplier=1.5):
        self.symbol = symbol
        self.fixed_sl_pct = fixed_sl_pct
        self.take_profit_pct = take_profit_pct
        self.trail_multiplier = trail_multiplier
        self.min_profit_activate = 0.0
        self.profit_lock_margin = 0.0
        self.entry_price = 0.0
        self.stop_loss_price = 0.0
        self.position_type = None
        self.quantity = 0.0
        self.trade_id = None
        self.active = False

    def _initial_stop(self, entry_price, side):
        if side == "long":
            return entry_price * (1 - self.fixed_sl_pct)
        else:
            return entry_price * (1 + self.fixed_sl_pct)

    def _check_take_profit(self, current_price):
        if self.position_type == "long":
            return current_price >= self.entry_price * (1 + self.take_profit_pct)
        else:
            return current_price <= self.entry_price * (1 - self.take_profit_pct)

    def initialize(self, entry_price, position_type, quantity, atr=None, adx=None, trade_id=None):
        self.entry_price = float(entry_price)
        self.position_type = position_type.lower()
        self.quantity = float(quantity)
        self.trade_id = str(trade_id)
        self.active = True
        self.stop_loss_price = self._initial_stop(self.entry_price, self.position_type)
        return self.trade_id

    def update(self, current_price, atr=None, spread=None, adx=None, trade_id=None):
        if not self.active:
            return

        if self._check_take_profit(current_price):
            logger.info(f"[{self.symbol}] Take-profit hit at {current_price}")
            self.active = False
            return "close"

        profit_pct = (
            (current_price - self.entry_price) / self.entry_price if self.position_type == "long"
            else (self.entry_price - current_price) / self.entry_price
        )

        if profit_pct < self.min_profit_activate:
            return

        if atr:
            base_trail = (atr / self.entry_price)
            trend_adj = 1.0 if adx < 25 else 0.75 if adx > 40 else 0.9
            spread_adj = 1.0 + (spread / self.entry_price * 5) if spread / self.entry_price > 0.001 else 1.0
            trail_pct = max(0.001, self.trail_multiplier * base_trail * trend_adj * spread_adj)
        else:
            trail_pct = 0.05

        if self.position_type == "long":
            new_stop = current_price * (1 - trail_pct)
            if new_stop > self.stop_loss_price:
                self.stop_loss_price = new_stop
        else:
            new_stop = current_price * (1 + trail_pct)
            if new_stop < self.stop_loss_price:
                self.stop_loss_price = new_stop

    def check_stop(self, current_price):
        if not self.active:
            return False
        if self.position_type == "long" and current_price <= self.stop_loss_price:
            return True
        if self.position_type == "short" and current_price >= self.stop_loss_price:
            return True
        return False

    def close_position(self):
        self.active = False

class DataPreprocessor:
    @staticmethod
    def normalize_lstm_input(arr):
        mean = arr.mean(axis=1, keepdims=True)
        std = arr.std(axis=1, keepdims=True) + 1e-9
        return (arr - mean) / std

    @staticmethod
    def prepare_lstm_input(df, window=100):
        required_cols = ['close', 'volume', 'RSI', 'MACD', 'ADX']
        df = df.copy()
        for col in required_cols:
            if col not in df.columns:
                df[col] = 0.0
        data = df[required_cols].tail(window).values.astype(float)
        if np.isnan(data).any():
            data = pd.DataFrame(data, columns=required_cols).ffill().bfill().values
        data = DataPreprocessor.normalize_lstm_input(data.T).T
        return data.reshape(1, window, len(required_cols))

class StrategySelector:
    @staticmethod
    def select_strategy_mode(adx, rsi, atr):
        if adx > 30:
            return "trend"
        elif adx < 15 and 40 < rsi < 60 and atr < 30:
            return "range"
        return "scalp"

    @staticmethod
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

class RiskManager:
    @staticmethod
    def historical_var(returns, alpha=0.05):
        if len(returns) < 10:
            return None
        return np.percentile(returns.dropna(), 100 * alpha)

    @staticmethod
    def parametric_var(returns, alpha=0.05):
        mu = returns.mean()
        sigma = returns.std()
        return mu + sigma * norm.ppf(alpha)

    @staticmethod
    def kelly_fraction(win_prob, win_loss_ratio):
        if win_loss_ratio <= 0:
            return 0.0
        return max(0.0, (win_prob - (1 - win_prob) / win_loss_ratio))

class MyTradingStrategy(bt.Strategy):
    params = (
        ('lstm_models', None),
        ('lstm_scalers', None),
        ('window', 100),
        ('rsi_period', 14),
        ('macd_fast', 12),
        ('macd_slow', 26),
        ('macd_signal', 9),
        ('adx_period', 14),
        ('ema20_period', 20),
        ('ema50_period', 50),
        ('atr_period', 14),
        ('roc_period', 5),
        ('spread', 0.0001),
    )

    def __init__(self):
        self.symbols = {data._name: data for data in self.datas}
        self.positions = {symbol: None for symbol in self.symbols}
        self.trailing_stops = {symbol: UltraAgressiveTrailingStop(symbol) for symbol in self.symbols}
        self.models = self.p.lstm_models
        self.scalers = self.p.lstm_scalers
        self.orders = {symbol: None for symbol in self.symbols}
        self.current_positions_count = 0

        self.indicators = {}
        for symbol, data in self.symbols.items():
            self.indicators[symbol] = {
                'rsi': btind.RSI(data.close, period=self.p.rsi_period),
                'macd': btind.MACD(data.close, period_me1=self.p.macd_fast, period_me2=self.p.macd_slow, period_signal=self.p.macd_signal),
                'adx': btind.ADX(data.high, data.low, data.close, period=self.p.adx_period),
                'ema20': btind.EMA(data.close, period=self.p.ema20_period),
                'ema50': btind.EMA(data.close, period=self.p.ema50_period),
                'atr': btind.ATR(data.high, data.low, data.close, period=self.p.atr_period),
                'roc': btind.ROC(data.close, period=self.p.roc_period),
            }

    def next(self):
        for symbol, data in self.symbols.items():
            if len(data) < self.p.window:
                continue

            df = pd.DataFrame({
                'close': data.close.get(size=self.p.window),
                'volume': data.volume.get(size=self.p.window),
                'RSI': self.indicators[symbol]['rsi'].get(size=self.p.window),
                'MACD': self.indicators[symbol]['macd'].macd.get(size=self.p.window),
                'ADX': self.indicators[symbol]['adx'].get(size=self.p.window),
            })

            action, new_position, confidence, confidence_factors = self.check_signal(
                symbol, df, self.positions[symbol], self.models.get(symbol), self.scalers.get(symbol)
            )

            current_price = data.close[0]
            atr = self.indicators[symbol]['atr'][0]
            adx = self.indicators[symbol]['adx'][0]

            if self.trailing_stops[symbol].active:
                self.trailing_stops[symbol].update(current_price, atr, self.p.spread, adx)
                if self.trailing_stops[symbol].check_stop(current_price):
                    self.close_position(symbol, current_price)

            if action == "buy" and self.current_positions_count < MAX_CONCURRENT_TRADES:
                size = self.calculate_position_size(symbol, current_price, atr)
                self.buy(data=data, size=size * LEVERAGE)
                self.trailing_stops[symbol].initialize(current_price, 'long', size, atr, adx, self.broker.getvalue())
                self.positions[symbol] = {'side': 'long', 'quantity': size, 'price': current_price}
                self.current_positions_count += 1
            elif action == "sell" and self.current_positions_count < MAX_CONCURRENT_TRADES:
                size = self.calculate_position_size(symbol, current_price, atr)
                self.sell(data=data, size=size * LEVERAGE)
                self.trailing_stops[symbol].initialize(current_price, 'short', size, atr, adx, self.broker.getvalue())
                self.positions[symbol] = {'side': 'short', 'quantity': size, 'price': current_price}
                self.current_positions_count += 1
            elif action in ["close_buy", "close_sell"]:
                self.close_position(symbol, current_price)

    def check_signal(self, symbol, df, current_position, model, scaler):
        if len(df) < 100:
            return "hold", current_position, 0.0, []

        rsi = df['RSI'].iloc[-1]
        macd = df['MACD'].iloc[-1]
        adx = df['ADX'].iloc[-1]
        ema20 = talib.EMA(df['close'], timeperiod=20).iloc[-1]
        ema50 = talib.EMA(df['close'], timeperiod=50).iloc[-1]
        atr = talib.ATR(df['high'], df['low'], df['close'], timeperiod=14).iloc[-1]
        close = df['close'].iloc[-1]
        roc = talib.ROC(df['close'], timeperiod=5).iloc[-1]

        strategy_mode = StrategySelector.select_strategy_mode(adx, rsi, atr)

        lstm_input = DataPreprocessor.prepare_lstm_input(df)
        prediction = model.predict(lstm_input, verbose=0)[0][0].item()

        dynamic_up, dynamic_down = StrategySelector.calculate_dynamic_thresholds(adx, strategy_mode)

        trend_up = ema20 > ema50
        trend_down = ema20 < ema50
        macd_bullish = macd > 0  # Simplified for backtest

        confidence_factors = []
        if prediction > 0.52 or prediction < 0.48:
            confidence_factors.append("LSTM strong")
        if (trend_up and macd > 0) or (trend_down and macd < 0):
            confidence_factors.append("MACD aligned")
        if rsi > 55 or rsi < 45:
            confidence_factors.append("RSI strong")

        action = "hold"
        new_position = current_position

        if strategy_mode == "trend":
            if trend_up and macd_bullish and prediction > dynamic_up:
                action = "buy"
            elif trend_down and not macd_bullish and prediction < dynamic_down:
                action = "sell"
        elif strategy_mode == "range":
            rolling_low = df['low'].rolling(window=20).min().iloc[-1]
            rolling_high = df['high'].rolling(window=20).max().iloc[-1]
            if close <= (rolling_low + 0.1 * atr) and prediction > dynamic_up:
                action = "buy"
            elif close >= (rolling_high - 0.1 * atr) and prediction < dynamic_down:
                action = "sell"
        elif strategy_mode == "scalp":
            if prediction > dynamic_up and roc > 0.5:
                action = "sell"
            elif prediction < dynamic_down and roc < -0.5:
                action = "buy"

        if current_position and current_position['side'] == "long" and (trend_down or roc < -1):
            action = "close_buy"
            new_position = None
        elif current_position and current_position['side'] == "short" and (trend_up or roc > 1):
            action = "close_sell"
            new_position = None

        confidence = len(confidence_factors) / 4.0  # Simple confidence score

        return action, new_position, confidence, confidence_factors

    def calculate_position_size(self, symbol, price, atr):
        available_margin = self.broker.getcash()
        raw_qty = (available_margin * 0.95 * LEVERAGE) / price
        win_prob = 0.55  # Assumed from bot
        win_loss_ratio = 2  # Assumed
        kelly = RiskManager.kelly_fraction(win_prob, win_loss_ratio)
        qty = raw_qty * max(kelly, 0.1)
        return qty

    def close_position(self, symbol, price):
        if self.positions[symbol]:
            self.close(data=self.symbols[symbol])
            self.trailing_stops[symbol].close_position()
            self.positions[symbol] = None
            self.current_positions_count -= 1

    def notify_order(self, order):
        if order.status in [order.Completed]:
            logger.info(f"Order completed for {order.data._name}: {order.getstatusname()}")

def create_sequences(data, labels, seq_len=100):
    X, y = [], []
    for i in range(len(data) - seq_len):
        X.append(data[i:i+seq_len])
        y.append(labels[i+seq_len-1])
    return np.array(X), np.array(y)

def create_gru_model(input_shape):
    model = Sequential([
        Input(shape=input_shape),
        Bidirectional(GRU(128, return_sequences=True, dropout=0.3)),  # Increased units
        Bidirectional(GRU(64, dropout=0.3)),
        Dense(32, activation='relu'),
        Dense(1, activation='sigmoid')
    ])
    model.compile(optimizer=keras.optimizers.Adam(learning_rate=0.0005),  # Lower LR
                  loss='binary_crossentropy',
                  metrics=['accuracy'])
    return model

def train_lstm_model(symbols_data, seq_len=100, epochs=50, batch_size=32):
    models = {}
    scalers = {}
    for symbol, data_path in symbols_data.items():
        if not os.path.exists(data_path):
            continue
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
            df = download_binance_data(symbol, interval=TIMEFRAME, api_key=api_key, api_secret=api_secret)
            if df is not None:
                os.makedirs(os.path.dirname(path), exist_ok=True)  # Ensure dir exists
                df.to_csv(path, index=False)
                logger.info(f"Saved {len(df)} rows to {path}")
            else:
                logger.warning(f"Skipping {symbol} due to download failure")
                continue

    lstm_models, lstm_scalers = train_lstm_model(symbols_data)

    cerebro = bt.Cerebro()
    cerebro.addstrategy(MyTradingStrategy, lstm_models=lstm_models, lstm_scalers=lstm_scalers)

    data_count = 0
    for symbol, data_path in symbols_data.items():
        try:
            if not os.path.exists(data_path):
                logger.warning(f"CSV missing for {symbol}, skipping")
                continue

            df = pd.read_csv(data_path)
            required_columns = ['datetime', 'open', 'high', 'low', 'close', 'volume']
            if not all(col in df.columns for col in required_columns):
                raise ValueError(f"Missing columns in {symbol} CSV: {set(required_columns) - set(df.columns)}")

            df['datetime'] = pd.to_datetime(df['datetime'])
            df = df.sort_values('datetime').ffill().bfill()

            # CORRECTION: Supprimé le filtre 1 mois pour utiliser tout le dataset (~3 mois)
            # Si vous voulez le garder: one_month_ago = datetime.datetime.now() - pd.DateOffset(months=1)
            # df = df[df['datetime'] >= one_month_ago]
            logger.info(f"Using {len(df)} rows for {symbol} from {df['datetime'].min()} to {df['datetime'].max()}")

            if df.empty:
                logger.warning(f"Empty dataframe for {symbol} after processing, skipping")
                continue

            df.to_csv(f'./backtest/data_cleaned_{symbol}.csv', index=False)

            compression = 60 if TIMEFRAME == '5m' else 5  # Adjusted for timeframe
            data = bt.feeds.PandasData(
                dataname=df,
                datetime='datetime',
                open='open',
                high='high',
                low='low',
                close='close',
                volume='volume',
                timeframe=bt.TimeFrame.Minutes,
                compression=compression,
                name=symbol
            )
            cerebro.adddata(data)
            data_count += 1
            logger.info(f"Data loaded for {symbol} ({len(df)} rows)")
        except Exception as e:
            logger.error(f"Error loading {symbol}: {e}")
            continue

    if data_count == 0:
        raise ValueError("No data sources added to Cerebro! Check CSV files and dates. Run with fresh download.")

    cerebro.broker.setcash(CAPITAL)
    cerebro.broker.setcommission(commission=0.0004)  # 0.04%
    cerebro.broker.set_slippage_perc(perc=0.001)  # 0.1%
    logger.info(f"Broker cash: {cerebro.broker.getcash():.2f} | Datas added: {data_count}")

    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe', timeframe=bt.TimeFrame.Days)
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='trades')
    cerebro.addanalyzer(bt.analyzers.SQN, _name='sqn')

    logger.info(f"Initial value: {cerebro.broker.getvalue():.2f}")
    results = cerebro.run()
    
    if not results:
        raise RuntimeError("Cerebro.run() returned empty results! Likely no data or strategy execution.")

    strategy = results[0]

    final_value = cerebro.broker.getvalue()
    logger.info(f"Final value: {final_value:.2f}")
    logger.info(f"Profit: {final_value - CAPITAL:.2f}")

    # [Reste des logs analyzers inchangé]
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
    API_KEY = 'tjmxSoMLy22RZEWO7sTbmMPa6t2wEHrYZElLAKep8VlAF1SbGlpVm9OIWGTIuJl6'
    API_SECRET = 'Z3hF9Tf83pP2E0KCx31n61hWxEdCTsMgNmVj35qzX68uaSyvXTo2wqmWBA8StR3N'

    symbols_data = {
        'BTCUSDT': './backtest/BTCUSDT_month.csv',
        'ETHUSDT': './backtest/ETHUSDT_month.csv',
        'XRPUSDT': './backtest/XRPUSDT_month.csv',
    }
    run_backtest(symbols_data)