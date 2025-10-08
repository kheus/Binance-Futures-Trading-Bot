<<<<<<< HEAD
﻿# 📈 Adaptive Trend Rider - Binance Futures Trading Bot

A high-performance trading bot for Binance Futures featuring:
- Advanced technical analysis with TA-Lib
- LSTM models for predictive signals
- Real-time data streaming via WebSockets and Kafka
- Robust database management with connection pooling
- Circuit breaker pattern for resilient API calls
- Comprehensive configuration management
- Built-in retry mechanisms with exponential backoff
- Monitoring and alerting integration

---

## ✨ Features

### Core Functionality
* **Real-Time Data Streaming**: WebSocket integration with Binance Futures API
* **Advanced Technical Analysis**: Comprehensive TA indicators via TA-Lib
* **Machine Learning**: LSTM models for predictive signal generation
* **Order Management**: Smart order execution with trailing stops

### Reliability & Resilience
* **Connection Pooling**: Efficient database connection management
* **Circuit Breaker**: Prevents cascading failures during API outages
* **Retry Mechanism**: Automatic retries with exponential backoff
* **Graceful Error Handling**: Comprehensive exception handling

### Monitoring & Operations
* **Real-time Monitoring**: Integration with Prometheus and Grafana
* **Alerting**: Telegram notifications for critical events
* **Logging**: Structured logging with rotation
* **Metrics**: Performance and trade metrics collection

### Configuration & Extensibility
* **Environment-Aware**: Supports development, testing, and production
* **Modular Design**: Easy to extend with new strategies
* **Testnet Support**: Safe testing with Binance Futures Testnet
* **Template Configs**: Easy setup with template files

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

1. Copy the template configuration files:
   ```bash
   cp config/config.yaml.template config/config.yaml
   cp .env.template .env
   ```

2. Edit the configuration files:
   - `config/config.yaml`: Main configuration
   - `.env`: Sensitive credentials and environment-specific settings

3. For production, set these environment variables:
   ```bash
   # Database
   export DB_PASSWORD=your_secure_password
   
   # Binance API
   export BINANCE_API_KEY=your_api_key
   export BINANCE_API_SECRET=your_api_secret
   
   # Trading parameters
   export TRADING_LEVERAGE=10
   export TRADING_MAX_RISK=0.02
   ```

4. Configure the database in `config/config.yaml`:
   ```yaml
   database:
     host: "localhost"
     port: 5432
     database: "trading_bot"
     user: "postgres"
     min_connections: 5
     max_connections: 20
   ```

5. Set up trading parameters:
   ```yaml
   trading:
     symbols: ["BTCUSDT", "ETHUSDT"]
     timeframe: "1h"
     capital: 1000.0
     max_risk_per_trade: 0.02
     trailing_stop: true
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
$env:PYTHONPATH = "C:\Users\Cheikh\binance-trading-bot"
$env:TF_ENABLE_ONEDNN_OPTS=0
python src/processing_core/main_bot.py
```

### 3. Launch the Dashboard

```bash
.\venv\Scripts\Activate.ps1
$env:PYTHONPATH = "C:\Users\Cheikh\binance-trading-bot"
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

```bash
git add C:\Users\Cheikh\binance-trading-bot\src\database\db_handler.py 
git add C:\Users\Cheikh\binance-trading-bot\src\database\schema.sql 
git add C:\Users\Cheikh\binance-trading-bot\src\performance\tracker.py 
git add C:\Users\Cheikh\binance-trading-bot\src\processing_core\main_bot.py 
git add C:\Users\Cheikh\binance-trading-bot\src\processing_core\lstm_model.py 
git add C:\Users\Cheikh\binance-trading-bot\src\processing_core\signal_generator.py 
git add C:\Users\Cheikh\binance-trading-bot\src\trade_execution\order_manager.py 
git add C:\Users\Cheikh\binance-trading-bot\src\trade_execution\sync_orders.py 
git add C:\Users\Cheikh\binance-trading-bot\src\trade_execution\ultra_aggressive_trailing.py 
git add C:\Users\Cheikh\binance-trading-bot\src\data_ingestion\data_formatter.py
git add C:\Users\Cheikh\binance-trading-bot\src\data_ingestion\kafka_consumer.py
git add C:\Users\Cheikh\binance-trading-bot\src\monitoring\metrics.py
git add C:\Users\Cheikh\binance-trading-bot\src\monitoring\alerting.py
git add C:\Users\Cheikh\binance-trading-bot\Readme.md 
git add C:\Users\Cheikh\binance-trading-bot\src\processing_core\indicators.py
git commit -m "Updates" 
git push -u origin master
```
