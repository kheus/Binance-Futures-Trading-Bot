# 📈 Adaptive Trend Rider

A powerful trading bot for Binance Futures, leveraging technical analysis (TA), LSTM models for signal generation, Kafka for real-time data ingestion, PostgreSQL for data storage, and Prometheus/Grafana/Telegram for monitoring and alerting. Designed for both simulated and live trading, this bot is built for performance and scalability.

---

## ✨ Features

* **Real-Time Data Ingestion**: Streams market data (candles) via Binance WebSocket and Kafka.
* **Technical Analysis**: Uses TA indicators (RSI, MACD, ADX, EMA, ATR, ROC) via TA-Lib.
* **LSTM-Powered Signals**: Generates buy/sell signals using deep learning models.
* **Order Execution**: Manages trades with trailing stops and enhanced order logic.
* **Data Storage**: Stores price data, trades, and signals in PostgreSQL.
* **Monitoring & Alerts**: Integrates with Prometheus for metrics, Grafana for dashboards, and Telegram for real-time alerts.
* **Configurable**: Fully customizable via YAML configuration files.
* **Testnet Support**: Safe testing with Binance Futures Testnet.

---

## 📋 Prerequisites

* Python 3.8+
* Kafka (local or hosted)
* PostgreSQL 16+
* Node.js (for dashboard frontend)
* Binance Futures API keys (Testnet or Live)
* Telegram bot token (for alerts)
* Prometheus and Grafana (optional)

---

## 🛠️ Setup

### 1. Clone the Repository

```bash
git clone https://github.com/kheus/Binance-Futures-Trading-Bot.git
cd Binance-Futures-Trading-Bot
```

### 2. Install Dependencies

```bash
# Python environment
python -m venv venv
source venv/bin/activate  # or .\venv\Scripts\Activate.ps1
pip install -r requirements.txt

# Node.js dashboard
npm install
```

### 3. Configure the Bot

Edit files inside the `config/` directory:

```yaml
# config/config.yaml
binance:
  api_key: "your_api_key"
  api_secret: "your_api_secret"
  base_url: "https://testnet.binancefuture.com"
  symbols: ["BTCUSDT", "ETHUSDT"]
  timeframe: "1h"
  capital: 1000
  leverage: 10

model:
  sequence_length: 10

# config/kafka_config.yaml
kafka:
  bootstrap_servers: "localhost:9092"

# config/db_config.yaml
database:
  host: "localhost"
  port: 5432
  database: "trading_bot_db"
  user: "postgres"
  password: "your_password"

# config/alerting_config.yaml
telegram:
  bot_token: "your_telegram_bot_token"
  chat_id: "your_chat_id"
prometheus:
  url: "http://localhost:8000"
```

### 4. Set Up Database

```bash
# Create DB
docker exec -it postgres psql -U postgres -c "CREATE DATABASE trading_bot_db"

# Initialize schema
psql -U postgres -d trading_bot_db -f src/database/schema.sql
```

### 5. Start Kafka

```bash
zookeeper-server-start.bat ./config/zookeeper.properties
kafka-server-start.bat ./config/server.properties
```

---

## 🚀 Running the Bot

### 1. Start Kafka Consumer

```bash
.\venv\Scripts\Activate.ps1
$env:TF_ENABLE_ONEDNN_OPTS=0
python src/data_ingestion/kafka_consumer.py
```

### 2. Run the Main Bot

```bash
.\venv\Scripts\Activate.ps1
$env:PYTHONUTF8=1
$env:PYTHONPATH="C:\Users\Cheikh\Binance-Futures-Trading-Bot"
$env:TF_ENABLE_ONEDNN_OPTS=0
python src/processing_core/main_bot.py
```

### 3. Launch the Dashboard

```bash
python dashboard.py
python app.py
npm run watch
```

Visit:

* Dashboard: [http://localhost:8000](http://localhost:8000)
* Prometheus Metrics: [http://localhost:8000/metrics](http://localhost:8000/metrics)

---

## 🛡️ Live Trading

> ⚠️ **Use Binance Testnet first** to verify the pipeline!

### To enable live trading:

* Change base URL to `https://fapi.binance.com`
* Uncomment actual order logic in `order_manager.py`
* Fund your Futures account

---

## 📊 Monitoring

* **Logs**: `logs/trading_bot.log`, `logs/lstm_model.log`
* **Metrics**: Exposed via Prometheus endpoint
* **Dashboards**: Visualized in Grafana
* **Alerts**: Sent to Telegram

---

## 🧪 Testing

To check open positions:

```bash
python -c "from binance.um_futures import UMFutures; import yaml; with open('config/config.yaml') as f: config = yaml.safe_load(f); client = UMFutures(key=config['binance']['api_key'], secret=config['binance']['api_secret'], base_url=config['binance']['base_url']); positions = client.get_position_information(); [print(f'{p['symbol']}: {p['positionAmt']} at {p['entryPrice']}') for p in positions if float(p['positionAmt']) != 0]"
```

---

## 📂 Project Structure

```
Binance-Futures-Trading-Bot/
├── config/              # YAML configuration
├── logs/                # Log files
├── models/              # Trained LSTM models
├── src/
│   ├── data_ingestion/  # Kafka consumers
│   ├── database/        # DB schema & handler
│   ├── processing_core/ # Bot logic, LSTM, signal generation
│   ├── trade_execution/ # Order manager, trailing stops
│   ├── performance/     # PnL tracker
│   ├── monitoring/      # Alerts & metrics
├── app.py               # Dashboard backend
├── requirements.txt     # Python dependencies
├── package.json         # Node.js dependencies
└── README.md
```

---

## 🌍 Contributing

```bash
git checkout -b feature/your-feature
# make changes
git add .
git commit -m "Add your feature"
git push origin feature/your-feature
```

Open a Pull Request and describe your changes.

---

## 🧯 Troubleshooting

| Issue       | Fix                                          |
| ----------- | -------------------------------------------- |
| Kafka error | Check `bootstrap_servers` in config          |
| DB error    | Ensure DB is created and credentials correct |
| No signals  | Look in `signal_generator.py` logs           |
| Model crash | Inspect data quality or `sequence_length`    |

---

## 📜 License

MIT License. See `LICENSE` for more details.

---

## 📬 Contact

Open an issue on GitHub or reach out via your configured Telegram bot.

---

**Happy Trading 🚀**
