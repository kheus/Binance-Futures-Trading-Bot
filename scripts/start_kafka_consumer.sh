#!/bin/bash
.\venv\Scripts\Activate.ps1
$env:PYTHONPATH = "C:\Users\Cheikh\binance-trading-bot"
$env:TF_ENABLE_ONEDNN_OPTS=0
python src\data_ingestion\kafka_consumer.py