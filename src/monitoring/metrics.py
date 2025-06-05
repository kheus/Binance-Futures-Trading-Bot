from prometheus_client import Counter, Gauge, start_http_server
import yaml

with open("config/alerting_config.yaml", "r") as f:
    config = yaml.safe_load(f)

trade_count = Counter("trade_count", "Total number of trades executed", ["symbol", "side"])
pnl_gauge = Gauge("trade_pnl", "Profit and Loss of trades", ["symbol"])

def start_metrics_server():
    try:
        start_http_server(config["prometheus"]["port"])
        print(f"Prometheus server started on port {config['prometheus']['port']}")
    except Exception as e:
        print(f"Prometheus server error: {e}")

def record_trade_metric(order_details):
    try:
        trade_count.labels(symbol=order_details["symbol"], side=order_details["side"]).inc()
        # Update pnl_gauge when PnL is calculated (e.g., after trade closure)
    except Exception as e:
        print(f"Metrics recording error: {e}")
