
import json
import yaml
import websocket  # Use the default import from websocket-client
from confluent_kafka import Producer
from datetime import datetime
import time

# Load configuration
with open("config/config.yaml", "r") as f:
    config = yaml.safe_load(f)
with open("config/kafka_config_local.yaml", "r") as f:
    kafka_config = yaml.safe_load(f)

SYMBOL = config["binance"]["symbol"]
TIMEFRAME = config["binance"]["timeframe"]
KAFKA_TOPIC = kafka_config["kafka"]["topic"]
KAFKA_BOOTSTRAP = kafka_config["kafka"]["bootstrap_servers"]

# Kafka producer
def create_kafka_producer():
    try:
        return Producer({
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "security.protocol": "PLAINTEXT",  # Explicitly set to PLAINTEXT
            "api.version.request": True,       # Enable ApiVersion requests
            "broker.version.fallback": "0.10.0"  # Fallback for older brokers if needed
        })
    except Exception as e:
        print(f"Kafka producer initialization error: {e}")
        return None

producer = create_kafka_producer()

def on_message(ws, message):
    try:
        data = json.loads(message)["k"]
        if data["x"]:  # Candle closed
            candle = {
                "timestamp": data["t"],
                "open": float(data["o"]),
                "high": float(data["h"]),
                "low": float(data["l"]),
                "close": float(data["c"]),
                "volume": float(data["v"]),
            }
            if producer:
                producer.produce(KAFKA_TOPIC, value=json.dumps(candle))
                producer.flush()
                print(f"Published candle to Kafka: {candle['timestamp']}")
    except Exception as e:
        print(f"Kafka publish error: {e}")

def on_error(ws, error):
    print(f"WebSocket error: {error}")

def on_close(ws, close_status_code, close_msg):
    print(f"WebSocket closed: {close_status_code}, {close_msg}")

def on_open(ws):
    print("WebSocket started")

def start_kafka_consumer():
    socket = f"wss://fstream.binance.com/ws/{SYMBOL.lower()}@kline_{TIMEFRAME}"
    while True:
        try:
            ws = websocket.WebSocketApp(socket,
                                       on_message=on_message,
                                       on_error=on_error,
                                       on_close=on_close,
                                       on_open=on_open)
            ws.run_forever(ping_interval=30, ping_timeout=10)
        except Exception as e:
            print(f"WebSocket connection failed: {e}")
            time.sleep(5)  # Wait before reconnecting
            continue

if __name__ == "__main__":
    if not producer:
        print("Failed to initialize Kafka producer. Exiting...")
        exit(1)
    start_kafka_consumer()
