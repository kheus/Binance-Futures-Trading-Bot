# adaptive-trend-rider

A trading bot for Binance Futures using technical analysis (TA) and LSTM for signal generation, Kafka for data ingestion, PostgreSQL for storage, and Prometheus/Grafana/Telegram for monitoring.
Setup

Install dependencies:
pip install -r requirements.txt


Configure:

Update config/config.yaml with Binance API keys and trading parameters.
Set Kafka settings in config/kafka_config.yaml.
Configure PostgreSQL in config/db_config.yaml.
Set Telegram and Prometheus details in config/alerting_config.yaml.


Initialize database:
bash scripts/setup_db.sh


Run Kafka consumer:
.\venv\Scripts\Activate.ps1
python src\data_ingestion\kafka_consumer.py 


Run bot:
.\venv\Scripts\Activate.ps1
$env:PYTHONUTF8 = 1
$env:PYTHONPATH = "C:\Users\Cheikh\binance-trading-bot"
python src\processing_core\main_bot.py

Check Data:
docker exec -it binance-trading-bot-postgres-1 psql -U postgres 
\c trading_bot
SELECT * FROM trades;

sqlite3 trading_bot.db



Replace everywhere by postgres:
Get-ChildItem -Recurse -File | ForEach-Object {
    (Get-Content $_.FullName) -replace 'postgres', 'postgres' | Set-Content $_.FullName
}

sqlite3 trading_bot.db
sqlite> SELECT * FROM trades LIMIT 10;

python app.py
npm run watch

Notes

Ensure Kafka and PostgreSQL services are running.
Simulated orders are enabled by default. Uncomment order placement code in order_manager.py for live trading.
Monitor metrics at [http://localhost:8000](http://localhost:8000) (Prometheus) and Grafana dashboards.


