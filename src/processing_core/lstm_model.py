import os
import numpy as np
import pandas as pd
from tensorflow.keras.models import Sequential, load_model
from tensorflow.keras.layers import LSTM, Dense, Input
from sklearn.preprocessing import MinMaxScaler

SEQ_LEN = 60
MODEL_PATH = "models/lstm_model.keras"

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
    scaler = MinMaxScaler()
    scaled_data = scaler.fit_transform(df[["close", "volume", "RSI", "MACD", "ADX"]].dropna())
    X, y = [], []
    for i in range(SEQ_LEN, len(scaled_data)):
        X.append(scaled_data[i-SEQ_LEN:i])
        y.append(1 if scaled_data[i, 0] > scaled_data[i-1, 0] else 0)
    return np.array(X), np.array(y), scaler

def train_or_load_model(df):
    # Create models directory if it doesn't exist
    os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)
    
    try:
        model = load_model(MODEL_PATH)
        print("Loaded existing LSTM model...")
        return model
    except Exception as e:
        print(f"Train/load model error: {e}")
        print("Training new LSTM model...")
        try:
            X, y, scaler = prepare_lstm_data(df)
            if len(X) == 0:
                print("Insufficient data for LSTM training.")
                return None
            model = build_lstm_model()
            model.fit(X, y, epochs=10, batch_size=32, validation_split=0.2, verbose=1)
            model.save(MODEL_PATH)
            print("Model trained and saved.")
            return model
        except Exception as e:
            print(f"Model training error: {e}")
            return None