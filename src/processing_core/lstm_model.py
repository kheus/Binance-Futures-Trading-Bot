import numpy as np
import pandas as pd
from tensorflow.keras.models import Sequential, load_model
from tensorflow.keras.layers import LSTM, Dense, Input
from tensorflow.keras.optimizers import Adam
from tensorflow.keras.callbacks import EarlyStopping
import os
import yaml

# Load configuration
with open("config/config.yaml", "r") as f:
    config = yaml.safe_load(f)

MODEL_PATH = config["model"]["path"].replace(".h5", ".keras")
SEQ_LEN = config["model"]["sequence_length"]

def prepare_lstm_data(df):
    try:
        df = df.dropna()
        features = df[["close", "volume", "RSI", "MACD", "ADX"]].values
        X, y = [], []
        for i in range(SEQ_LEN, len(features)):
            X.append(features[i-SEQ_LEN:i])
            y.append(1 if features[i][0] > features[i-1][0] else 0)
        return np.array(X), np.array(y)
    except Exception as e:
        print(f"LSTM data preparation error: {e}")
        return np.array([]), np.array([])

def build_lstm_model(input_shape):
    try:
        model = Sequential()
        model.add(Input(shape=input_shape))
        model.add(LSTM(50, return_sequences=True))
        model.add(LSTM(50))
        model.add(Dense(25, activation="relu"))
        model.add(Dense(1, activation="sigmoid"))
        model.compile(optimizer=Adam(learning_rate=0.001), loss="binary_crossentropy", metrics=["accuracy"])
        return model
    except Exception as e:
        print(f"LSTM model build error: {e}")
        return None

def train_or_load_model(df):
    try:
        if os.path.exists(MODEL_PATH):
            print("Loading existing LSTM model...")
            return load_model(MODEL_PATH)
        print("Training new LSTM model...")
        X, y = prepare_lstm_data(df)
        if X.size == 0 or y.size == 0:
            print("Insufficient data for training")
            return None
        model = build_lstm_model((SEQ_LEN, X.shape[2]))
        early_stop = EarlyStopping(monitor="loss", patience=3)
        model.fit(X, y, epochs=20, batch_size=32, callbacks=[early_stop], verbose=0)
        model.save(MODEL_PATH)
        print("Model trained and saved.")
        return model
    except Exception as e:
        print(f"Train/load model error: {e}")
        return None