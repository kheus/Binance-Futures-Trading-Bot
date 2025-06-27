# module: monitoring.metrics
from prometheus_client import Counter, Gauge, start_http_server
import yaml
import logging

logger = logging.getLogger(__name__)

with open("config/alerting_config.yaml", "r", encoding="utf-8-sig") as f:
    config = yaml.safe_load(f)

trade_count = Counter("trade_count", "Total number of trades executed", ["symbol", "side"])
pnl_gauge = Gauge("trade_pnl", "Profit and Loss of trades", ["symbol"])

def start_metrics_server():
    try:
        start_http_server(config["prometheus"]["port"])
        logger.info(f"Prometheus server started on port {config['prometheus']['port']}")
    except Exception as e:
        logger.error(f"Prometheus server error: {e}")

def record_trade_metric(order_details):
    try:
        trade_count.labels(symbol=order_details["symbol"], side=order_details["side"]).inc()
        pnl_gauge.labels(symbol=order_details["symbol"]).set(order_details.get("pnl", 0))
    except Exception as e:
        logger.error(f"Metrics recording error: {e}")
