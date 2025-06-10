import os
import numpy as np
import pandas as pd
from tensorflow.keras.models import Sequential, load_model
from tensorflow.keras.layers import LSTM, Dense, Input
from sklearn.preprocessing import MinMaxScaler
import logging
import time

SEQ_LEN = 100
MODEL_PATH = "models/lstm_model.keras"
logger = logging.getLogger(__name__)

def build_lstm_model(input_shape=(SEQ_LEN, 5)):
    model = Sequential([
        Input(shape=input_shape),
        LSTM(50, return_sequences=True),
        LSTM(50),
        Dense(25, activation='relu'),
        Dense(1, activation='sigmoid')
    ])
    model.compile(optimizer='adam', loss='binary_crossentropy', metrics=['accuracy'])
    return model

def prepare_lstm_data(df):
    logger.info(f"[Model] Preparing LSTM data with shape {df.shape}")
    required_cols = ["close", "volume", "RSI", "MACD", "ADX"]
    if not all(col in df.columns for col in required_cols):
        logger.error(f"[Model] Missing columns: {required_cols}")
        return None, None, None
    df = df[required_cols].dropna()
    if len(df) < SEQ_LEN + 1:
        logger.error(f"[Model] Insufficient data: {len(df)} rows, need {SEQ_LEN + 1}")
        return None, None, None
    scaler = MinMaxScaler()
    scaled_data = scaler.fit_transform(df)
    X, y = [], []
    for i in range(SEQ_LEN, len(scaled_data)):
        X.append(scaled_data[i-SEQ_LEN:i])
        y.append(1 if scaled_data[i, 0] > scaled_data[i-1, 0] else 0)  # Hausse basée sur close
    X, y = np.array(X), np.array(y)
    logger.info(f"[Model] Prepared X shape: {X.shape}, y shape: {y.shape}")
    return X, y, scaler

def train_or_load_model(df):
    os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)
    logger.info(f"[Model] Attempting to train or load model with data shape {df.shape}")
    
    try:
        model = load_model(MODEL_PATH)
        logger.info(f"[Model] Loaded existing model from {MODEL_PATH}")
        model.last_train_time = time.time()  # Ajout d'un attribut pour suivi
        return model
    except Exception as e:
        logger.warning(f"[Model] Failed to load model: {e}, training new model")

    try:
        X, y, scaler = prepare_lstm_data(df)
        if X is None or len(X) == 0:
            logger.error("[Model] No valid data for training")
            return None
        model = build_lstm_model()
        logger.info(f"[Model] Training with X shape {X.shape}, y shape {y.shape}")
        model.fit(X, y, epochs=10, batch_size=32, validation_split=0.2, verbose=1)
        model.save(MODEL_PATH)
        model.last_train_time = time.time()
        logger.info(f"[Model] Trained and saved model at {MODEL_PATH}")
        return model
    except Exception as e:
        logger.error(f"[Model] Training failed: {e}")
        return None