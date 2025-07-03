import os
import logging
import numpy as np
import pandas as pd
from binance.client import Client
from tensorflow.keras.models import Sequential, load_model
from tensorflow.keras.layers import LSTM, Dense, Input
from tensorflow.keras.callbacks import EarlyStopping
from sklearn.preprocessing import MinMaxScaler
import json
import tensorflow as tf
import yaml
from pathlib import Path
from datetime import datetime
import time
from src.processing_core.indicators import calculate_indicators
from src.database.db_handler import execute_query  # Import to fetch metrics

# Charger la configuration YAML
CONFIG_PATH = Path(__file__).parent.parent.parent / "config" / "config.yaml"
with open(CONFIG_PATH, "r", encoding="utf-8-sig") as f:
    config = yaml.safe_load(f)

# Configuration générale
SEQ_LEN = config["model"]["sequence_length"]
MODEL_DIR = Path(__file__).parent.parent.parent / "models"
API_KEY = config["binance"]["api_key"]
API_SECRET = config["binance"]["api_secret"]
SYMBOLS = config["binance"]["symbols"]
INTERVAL = config["binance"].get("timeframe", "1h")
LIMIT = config["binance"].get("limit", 500)

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(name)s] %(message)s',
    handlers=[logging.FileHandler("logs/lstm_model.log", encoding='utf-8'), logging.StreamHandler()]
)

def fetch_binance_data(symbol, interval, limit):
    """Fetch historical data from Binance."""
    try:
        client = Client(API_KEY, API_SECRET)
        klines = client.get_klines(symbol=symbol, interval=interval, limit=limit)
        df = pd.DataFrame(klines, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_asset_volume', 'trades', 'taker_buy_base', 'taker_buy_quote', 'ignored'])
        df['close'] = df['close'].astype(float)
        df['volume'] = df['volume'].astype(float)
        df['open'] = df['open'].astype(float)
        df['high'] = df['high'].astype(float)
        df['low'] = df['low'].astype(float)
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df.set_index('timestamp', inplace=True)
        logger.info(f"[Data] Fetched {len(df)} {interval} candles for {symbol}")
        return df[['open', 'high', 'low', 'close', 'volume']]
    except Exception as e:
        logger.error(f"[Data] Failed to fetch Binance data for {symbol}: {e}")
        return None

def fetch_metrics(symbol, timestamps):
    """Fetch RSI, MACD, ADX from metrics table for given timestamps."""
    try:
        timestamps = [int(t) for t in timestamps]
        query = """
        SELECT timestamp, rsi, macd, adx
        FROM metrics
        WHERE symbol = %s AND timestamp = ANY(%s)
        """
        result = execute_query(query, (symbol, timestamps), fetch=True)
        if result:
            metrics_df = pd.DataFrame(result, columns=['timestamp', 'rsi', 'macd', 'adx'])
            metrics_df['timestamp'] = metrics_df['timestamp'].astype(np.int64)
            logger.info(f"[Model] Fetched {len(metrics_df)} metrics for {symbol}")
            return metrics_df
        logger.warning(f"[Model] No metrics found for {symbol} at provided timestamps")
        return pd.DataFrame(columns=['timestamp', 'rsi', 'macd', 'adx'])
    except Exception as e:
        logger.error(f"[Model] Error fetching metrics for {symbol}: {str(e)}")
        return pd.DataFrame(columns=['timestamp', 'rsi', 'macd', 'adx'])

def prepare_lstm_data(df):
    """Prepare LSTM sequences with refined labeling."""
    logger.info(f"[Model] Preparing LSTM data with shape {df.shape}")
    required_cols = ["close", "volume", "rsi", "macd", "adx"]
    if not all(col in df.columns for col in required_cols):
        logger.error(f"[Model] Missing columns: {set(required_cols) - set(df.columns)}")
        return None, None, None
    df = df[required_cols].dropna()
    if len(df) <= SEQ_LEN + 5:  # Need extra rows for future labeling
        logger.error(f"[Model] Insufficient data: {len(df)} rows, need {SEQ_LEN + 6}")
        return None, None, None
    scaler = MinMaxScaler()
    scaled_data = scaler.fit_transform(df)
    X, y = [], []
    for i in range(SEQ_LEN, len(scaled_data) - 5):
        X.append(scaled_data[i-SEQ_LEN:i])
        future_avg = np.mean(scaled_data[i+1:i+6, 0])  # Average of next 5 closes
        current = scaled_data[i, 0]
        y.append(1 if future_avg > current else 0)
    X, y = np.array(X), np.array(y)
    if len(X) == 0:
        logger.warning("[Model] No sequences generated")
        return None, None, None
    logger.info(f"[Model] Prepared X shape: {X.shape}, y shape: {y.shape}")
    return X, y, scaler

def augment_data(X, y):
    """Augment data with Gaussian noise."""
    X_noisy = X + np.random.normal(0, 0.01, X.shape)
    return np.concatenate([X, X_noisy]), np.concatenate([y, y])

def build_lstm_model(input_shape=(SEQ_LEN, 5)):
    """Build and compile the LSTM model."""
    model = Sequential([
        Input(shape=input_shape),
        LSTM(64, return_sequences=True, dropout=0.3),
        LSTM(32, dropout=0.3),
        Dense(16, activation='relu'),
        Dense(1, activation='sigmoid')
    ])
    model.compile(
        optimizer=tf.keras.optimizers.Adam(learning_rate=0.001),
        loss='binary_crossentropy',
        metrics=['accuracy', tf.keras.metrics.Precision()]
    )
    return model

def train_or_load_model(df, symbol):
    """Train or load the LSTM model with real data for a specific symbol."""
    os.makedirs(MODEL_DIR, exist_ok=True)
    model_path = MODEL_DIR / f"lstm_model_{symbol}.keras"
    meta_path = MODEL_DIR / f"lstm_model_{symbol}_meta.json"
    logger.info(f"[Model] Attempting to train or load model for {symbol} with data shape {df.shape}")

    # Log DataFrame columns for debugging
    logger.info(f"[Model] DataFrame columns: {df.columns.tolist()}")

    # Add technical indicators by fetching from metrics table
    try:
        df = df.copy()
        # Reset index to avoid timestamp ambiguity
        df = df.reset_index()
        df['timestamp'] = (df['timestamp'].astype(np.int64) // 10**6).astype(np.int64)  # Convert to milliseconds
        metrics_df = fetch_metrics(symbol, df['timestamp'].values)
        if not metrics_df.empty:
            df = df.merge(metrics_df, on='timestamp', how='left')
            df.set_index(pd.to_datetime(df['timestamp'] * 10**6), inplace=True)  # Restore DatetimeIndex
            df = df.drop(columns=['timestamp'])
            logger.info(f"[Model] Merged metrics for {symbol}, new columns: {df.columns.tolist()}")
        else:
            logger.warning(f"[Model] No metrics found in database for {symbol}, falling back to calculate_indicators")
            df = calculate_indicators(df.set_index(pd.to_datetime(df['timestamp'] * 10**6)), symbol)
            if df is None or not all(col in df.columns for col in ['rsi', 'macd', 'adx']):
                logger.error(f"[Model] Failed to calculate indicators for {symbol}")
                return None, None
    except Exception as e:
        logger.error(f"[Model] Error merging metrics for {symbol}: {str(e)}")
        return None, None

    # Ensure all required columns are present
    required_cols = ["close", "volume", "rsi", "macd", "adx"]
    missing_cols = set(required_cols) - set(df.columns)
    if missing_cols:
        logger.error(f"[Model] Missing required columns after processing: {missing_cols}")
        return None, None

    # Fill missing values
    df = df.ffill().bfill()
    if df.isna().any().any():
        logger.error(f"[Model] Data contains NaN after filling for {symbol}")
        return None, None

    # Load metadata if exists
    meta = {}
    if meta_path.exists():
        try:
            with open(meta_path, 'r', encoding="utf-8-sig") as f:
                meta = json.load(f)
        except Exception as e:
            logger.warning(f"[Model] Failed to load metadata for {symbol}: {e}, will train new model")

    try:
        if model_path.exists() and meta.get('scaler', {}).get('scale_') and meta.get('scaler', {}).get('min_'):
            model = load_model(model_path)
            scaler = MinMaxScaler()
            scaler.scale_ = np.array(meta['scaler']['scale_'])
            scaler.min_ = np.array(meta['scaler']['min_'])
            last_train_time = meta.get('last_train_time', 0)
            last_train_close = meta.get('last_train_close', 0)
            current_close = df['close'].iloc[-1]
            if last_train_time and abs(current_close - last_train_close) < 1.0:
                logger.info(f"[Model] Data unchanged since {datetime.fromtimestamp(last_train_time).strftime('%Y-%m-%d %H:%M:%S')}, reusing model")
                return model, scaler
            logger.info(f"[Model] Data changed, retraining model")
        else:
            logger.info(f"[Model] No valid model or metadata found for {symbol}, training new model")
    except Exception as e:
        logger.warning(f"[Model] Failed to load model for {symbol}: {e}, training new model")

    try:
        X, y, scaler = prepare_lstm_data(df)
        if X is None or len(X) < 10:  # Minimum sequences for training
            logger.error(f"[Model] Insufficient valid data for training {symbol}")
            return None, None
        X, y = augment_data(X, y)
        model = build_lstm_model()
        logger.info(f"[Model] Training with X shape {X.shape}, y shape {y.shape}")
        early_stopping = EarlyStopping(monitor='val_loss', patience=5, restore_best_weights=True)
        history = model.fit(
            X, y,
            validation_split=0.1,
            epochs=100,
            batch_size=32,
            verbose=1,
            callbacks=[early_stopping]
        )
        model.save(model_path)
        current_time = time.time()
        meta = {
            'last_train_time': current_time,
            'last_train_close': float(df['close'].iloc[-1]),
            'symbol': symbol,
            'scaler': {
                'scale_': scaler.scale_.tolist(),
                'min_': scaler.min_.tolist()
            }
        }
        with open(meta_path, 'w', encoding="utf-8") as f:
            json.dump(meta, f)
        logger.info(f"[Model] Training completed for {symbol} with {len(df)} rows")
        return model, scaler
    except Exception as e:
        logger.error(f"[Model] Training failed for {symbol}: {e}")
        return None, None

if __name__ == "__main__":
    # Vérifier que SYMBOLS est bien défini
    if not SYMBOLS or not isinstance(SYMBOLS, list) or len(SYMBOLS) == 0:
        logger.error("[Main] Invalid SYMBOLS configuration")
        raise ValueError("SYMBOLS is not properly defined in config.yaml")
    
    # Fetch real data from Binance for the first symbol
    symbol = SYMBOLS[0]
    logger.info(f"[Main] Processing symbol: {symbol}")
    df = fetch_binance_data(symbol, INTERVAL, LIMIT)
    if df is not None:
        model, scaler = train_or_load_model(df, symbol=symbol)
        if model:
            logger.info("[Main] Model training or loading successful")
        else:
            logger.error("[Main] Model training or loading failed")
    else:
        logger.error("[Main] Failed to proceed due to data fetch failure")