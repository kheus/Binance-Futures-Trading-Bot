Adaptive Trend Rider

A powerful trading bot for Binance Futures, leveraging technical analysis (TA), LSTM models for signal generation, Kafka for real-time data ingestion, PostgreSQL for data storage, and Prometheus/Grafana/Telegram for monitoring and alerting. Designed for both simulated and live trading, this bot is built for performance and scalability.

✨ Features

Real-Time Data Ingestion: Streams market data (candles) via Binance WebSocket and Kafka.
Technical Analysis: Uses TA indicators (RSI, MACD, ADX, EMA, ATR, ROC) via talib.
LSTM-Powered Signals: Generates buy/sell signals using deep learning models.
Order Execution: Manages trades with trailing stops and enhanced order logic.
Data Storage: Stores price data, trades, and signals in PostgreSQL.
Monitoring & Alerts: Integrates with Prometheus for metrics, Grafana for dashboards, and Telegram for real-time alerts.
Configurable: Fully customizable via YAML configuration files.
Testnet Support: Safe testing with Binance Futures Testnet.


📋 Prerequisites

Python 3.8+
Kafka (local or hosted, e.g., Confluent Cloud)
PostgreSQL 16+
Node.js (for dashboard frontend)
Binance Futures API keys (Testnet or Live)
Telegram bot token (for alerts)
Prometheus and Grafana (optional, for monitoring)


🛠️ Setup
1. Clone the Repository
git clone https://github.com/kheus/Binance-Futures-Trading-Bot.git
cd Binance-Futures-Trading-Bot

2. Install Dependencies
Create a virtual environment and install Python dependencies:
python -m venv venv
.\venv\Scripts\Activate.ps1  # Windows
# or
source venv/bin/activate  # Linux/Mac
pip install -r requirements.txt

Install Node.js dependencies for the dashboard:
npm install

3. Configure the Bot
Update the configuration files in the config/ directory:

config/config.yaml: Set Binance API keys, trading parameters (symbols, timeframe, capital, leverage), and model settings.binance:
  api_key: "your_api_key"
  api_secret: "your_api_secret"
  base_url: "https://testnet.binancefuture.com"  # Use for Testnet
  symbols: ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"]
  timeframe: "1h"
  capital: 1000
  leverage: 10
model:
  sequence_length: 10


config/kafka_config.yaml: Configure Kafka bootstrap servers.kafka:
  bootstrap_servers: "localhost:9092"


config/db_config.yaml: Set PostgreSQL connection details.database:
  host: "localhost"
  port: 5432
  database: "trading_bot_db"
  user: "postgres"
  password: "your_password"


config/alerting_config.yaml: Add Telegram and Prometheus settings.telegram:
  bot_token: "your_telegram_bot_token"
  chat_id: "your_chat_id"
prometheus:
  url: "http://localhost:8000"



4. Set Up Database
Start PostgreSQL and create the database:
& "C:\Program Files\PostgreSQL\16\bin\psql.exe" -U postgres -c "CREATE DATABASE trading_bot_db"

Initialize the database schema:
& "C:\Program Files\PostgreSQL\16\bin\psql.exe" -U postgres -d trading_bot_db -f src\database\schema.sql

5. Start Kafka
Ensure Kafka is running (e.g., via Docker or a local installation). For a local setup:
# Start ZooKeeper
zookeeper-server-start.bat .\config\zookeeper.properties

# Start Kafka server
kafka-server-start.bat .\config\server.properties


🚀 Running the Bot
1. Start the Kafka Consumer
The Kafka consumer fetches real-time candle data from Binance and publishes it to Kafka topics.
.\venv\Scripts\Activate.ps1
$env:TF_ENABLE_ONEDNN_OPTS=0
python src\data_ingestion\kafka_consumer.py

2. Run the Main Bot
The main bot processes data, generates signals, executes trades, and monitors performance.
.\venv\Scripts\Activate.ps1
$env:PYTHONUTF8=1
$env:PYTHONPATH="C:\Users\Cheikh\Binance-Futures-Trading-Bot"
$env:TF_ENABLE_ONEDNN_OPTS=0
python src\processing_core\main_bot.py

3. Launch the Dashboard
Run the dashboard backend and frontend for monitoring:
python app.py
npm run watch

Access the dashboard at http://localhost:8000 (Prometheus) and configure Grafana to visualize metrics.

🛡️ Live Trading

⚠️ Warning: Live trading involves financial risk. Test thoroughly on Binance Futures Testnet before enabling live trading.

To enable live trading:

Update config/config.yaml to use the live Binance Futures API:binance:
  base_url: "https://fapi.binance.com"


Uncomment the order placement logic in src/trade_execution/order_manager.py:# client.new_order(**order_params)


Ensure sufficient funds in your Binance Futures account.

By default, the bot uses simulated orders for safety.

📊 Monitoring

Logs: Check logs/trading_bot.log and logs/lstm_model.log for detailed runtime information.
Prometheus: Metrics are exposed at http://localhost:8000.
Grafana: Configure dashboards to visualize trading metrics (e.g., PNL, signal confidence).
Telegram: Receive real-time alerts for trades, errors, and model updates.


🧪 Testing
Run the bot on Binance Futures Testnet (https://testnet.binancefuture.com) to validate functionality. Use the following command to verify open positions:
python -c "from binance.um_futures import UMFutures; import yaml; with open('config/config.yaml', 'r', encoding='utf-8-sig') as f: config = yaml.safe_load(f); client = UMFutures(key=config['binance']['api_key'], secret=config['binance']['api_secret'], base_url=config['binance']['base_url']); positions = client.get_position_information(); [print(f'Open position for {p['symbol']}: {p['positionAmt']} at {p['entryPrice']}') for p in positions if p['symbol'] in config['binance']['symbols'] and float(p['positionAmt']) != 0]"


📂 Project Structure
Binance-Futures-Trading-Bot/
├── config/                     # Configuration files (YAML)
├── logs/                       # Log files
├── models/                     # LSTM model files (.keras, .json)
├── src/
│   ├── data_ingestion/         # Kafka consumer and data formatting
│   ├── database/               # PostgreSQL schema and DB handlers
│   ├── processing_core/        # Main bot, LSTM model, signal generation
│   ├── trade_execution/        # Order management and trailing stops
│   ├── performance/            # Performance tracking
│   ├── monitoring/             # Prometheus and Telegram integration
├── app.py                      # Dashboard backend
├── requirements.txt            # Python dependencies
├── package.json                # Node.js dependencies
└── Readme.md                   # Project documentation


🌐 Contributing
Contributions are welcome! To contribute:

Fork the repository: https://github.com/kheus/Binance-Futures-Trading-Bot
Create a feature branch:git checkout -b feature/your-feature


Commit your changes:git commit -m "Add your feature"


Push to the branch:git push origin feature/your-feature


Open a pull request.

To push updates to the main branch:
git add .
git commit -m "Your commit message"
git push -u origin main

git add C:\Users\Cheikh\binance-trading-bot\src\database\db_handler.py 
git add C:\Users\Cheikh\binance-trading-bot\src\database\schema.sql 
git add C:\Users\Cheikh\binance-trading-bot\src\performance\tracker.py 
git add C:\Users\Cheikh\binance-trading-bot\src\processing_core\main_bot.py 
git add C:\Users\Cheikh\binance-trading-bot\src\processing_core\lstm_model.py 
git add C:\Users\Cheikh\binance-trading-bot\src\processing_core\signal_generator.py 
git add C:\Users\Cheikh\binance-trading-bot\src\trade_execution\order_manager.py 
git add C:\Users\Cheikh\binance-trading-bot\src\trade_execution\sync_orders.py 
git add C:\Users\Cheikh\binance-trading-bot\src\trade_execution\ultra_aggressive_trailing.py 
git add C:\Users\Cheikh\binance-trading-bot\Readme.md 
git commit -m "Updates" 
git push -u origin master

⚠️ Troubleshooting

Kafka connection issues: Ensure Kafka is running and bootstrap_servers in kafka_config.yaml is correct.
PostgreSQL errors: Verify database credentials in db_config.yaml and ensure the trading_bot_db database exists.
No signals generated: Check logs for errors in signal_generator.py and adjust thresholds if needed.
Model training fails: Increase limit in config.yaml or check data quality in lstm_model.py.

For detailed logs:
Get-Content -Path logs\trading_bot.log -Tail 50
Get-Content -Path logs\lstm_model.log -Tail 50


📜 License
This project is licensed under the MIT License. See the LICENSE file for details.

📬 Contact
For issues or questions, open an issue on GitHub or contact via Telegram (configured in alerting_config.yaml).
Happy trading! 🚀
