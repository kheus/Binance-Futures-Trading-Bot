# src/data_ingestion/kafka_consumer.py
import json
import yaml
import websocket
from confluent_kafka import Producer, admin
from datetime import datetime
import time
import threading
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load configuration
with open("config/config.yaml", "r", encoding="utf-8-sig") as f:
    config = yaml.safe_load(f)
with open("config/kafka_config_local.yaml", "r", encoding="utf-8-sig") as f:
    kafka_config = yaml.safe_load(f)

SYMBOLS = config["binance"]["symbols"]
TIMEFRAME = config["binance"]["timeframe"]
KAFKA_BOOTSTRAP = kafka_config["kafka"]["bootstrap_servers"]

# Kafka producer
def create_kafka_producer():
    try:
        producer = Producer({
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "security.protocol": "PLAINTEXT",
            "api.version.request": True,
            "broker.version.fallback": "0.10.0"
        })
        logger.info(f"[Kafka] Producer initialized with bootstrap servers: {KAFKA_BOOTSTRAP}")
        return producer
    except Exception as e:
        logger.error(f"[Kafka] Producer initialization error: {e}")
        return None

producer = create_kafka_producer()

# Create topics if they don't exist
def create_topics():
    if not producer:
        logger.error("[Kafka] Cannot create topics, producer not initialized")
        return
    try:
        admin_client = admin.AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
        topics = [f"{symbol}_candle" for symbol in SYMBOLS]
        existing_topics = set(admin_client.list_topics(timeout=10).topics.keys())
        new_topics = [t for t in topics if t not in existing_topics]
        if new_topics:
            futures = admin_client.create_topics(
                [admin.NewTopic(t, num_partitions=1, replication_factor=1) for t in new_topics],
                operation_timeout=30
            )
            for topic, future in futures.items():
                try:
                    future.result(timeout=30)  # Wait for topic creation
                    logger.info(f"[Kafka] Created topic: {topic}")
                except Exception as e:
                    logger.error(f"[Kafka] Failed to create topic {topic}: {e}")
            # Verify topic creation
            time.sleep(2)  # Allow metadata to propagate
            existing_topics = set(admin_client.list_topics(timeout=10).topics.keys())
            failed_topics = [t for t in new_topics if t not in existing_topics]
            if failed_topics:
                logger.error(f"[Kafka] Failed to verify creation of topics: {failed_topics}")
            else:
                logger.info(f"[Kafka] Successfully created and verified topics: {new_topics}")
        else:
            logger.info("[Kafka] All required topics already exist")
    except Exception as e:
        logger.error(f"[Kafka] Failed to create topics: {e}")
        raise

create_topics()

def on_message(ws, message, symbol):
    try:
        data = json.loads(message)["k"]
        if data["x"]:  # Candle closed
            timestamp_ms = data.get("T") or data.get("t")
            candle = {
                "timestamp": timestamp_ms,
                "open": float(data["o"]),
                "high": float(data["h"]),
                "low": float(data["l"]),
                "close": float(data["c"]),
                "volume": float(data["v"]),
            }
            topic = f"{symbol}_candle"
            if producer:
                producer.produce(topic, value=json.dumps(candle))
                producer.flush()
                logger.info(f"[Kafka] Published candle to topic {topic} for {symbol}: {candle}")
            else:
                logger.warning(f"[Kafka] No producer available for {symbol}")
    except Exception as e:
        logger.error(f"[Kafka] Publish error for {symbol}: {e}")

def on_error(ws, error, symbol):
    logger.error(f"[WebSocket] Error for {symbol}: {error}")

def on_close(ws, close_status_code, close_msg, symbol):
    logger.warning(f"[WebSocket] Closed for {symbol}: {close_status_code}, {close_msg}")

def on_open(ws, symbol):
    logger.info(f"[WebSocket] Started for {symbol}")

def start_websocket_for_symbol(symbol):
    socket = f"wss://fstream.binance.com/ws/{symbol.lower()}@kline_{TIMEFRAME}"
    logger.info(f"[WebSocket] Connecting to {socket}")
    while True:
        try:
            ws = websocket.WebSocketApp(
                socket,
                on_message=lambda ws, msg: on_message(ws, msg, symbol),
                on_error=lambda ws, error: on_error(ws, error, symbol),
                on_close=lambda ws, csc, cm: on_close(ws, csc, cm, symbol),
                on_open=lambda ws: on_open(ws, symbol)
            )
            ws.run_forever(ping_interval=30, ping_timeout=10)
        except Exception as e:
            logger.error(f"[WebSocket] Connection failed for {symbol}: {e}")
            time.sleep(5)
            continue

if __name__ == "__main__":
    if not producer:
        logger.error("[Main] Failed to initialize Kafka producer. Exiting...")
        exit(1)

    # Start a WebSocket connection for each symbol in separate threads
    threads = []
    for symbol in SYMBOLS:
        thread = threading.Thread(target=start_websocket_for_symbol, args=(symbol,))
        threads.append(thread)
        thread.start()

    # Allow threads to run independently
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        logger.info("[Main] Script terminated by user")
        for thread in threads:
            thread.join()
