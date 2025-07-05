# module: monitoring.metrics
from prometheus_client import Counter, Gauge, start_http_server
import yaml
import logging
from binance.um_futures import UMFutures
import talib
import pandas as pd

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

def get_current_atr(client: UMFutures, symbol: str) -> float:
    """
    Calcule l'ATR sur 14 périodes 1h. Utilise une approximation si les données sont insuffisantes.
    Retourne l'ATR (float) ou None si échec.
    """
    try:
        logger.debug(f"[{symbol}] 📊 Récupération des bougies pour ATR...")
        klines = client.klines(symbol=symbol, interval='1h', limit=21)  # 21 pour lisser talib.ATR

        if len(klines) < 15:
            logger.warning(f"[{symbol}] ⚠️ Données insuffisantes pour ATR (seulement {len(klines)} bougies). Fallback: 1% du prix.")
            price = float(client.ticker_price(symbol=symbol)['price'])
            return round(price * 0.01, 2)

        df = pd.DataFrame(klines, columns=[
            "open_time", "open", "high", "low", "close", "volume",
            "close_time", "quote_asset_vol", "num_trades", "taker_buy_base_vol",
            "taker_buy_quote_vol", "ignore"
        ])
        df = df[["high", "low", "close"]].astype(float)

        if df.isnull().any().any():
            logger.error(f"[{symbol}] ❌ Données corrompues pour ATR. Fallback.")
            price = float(client.ticker_price(symbol=symbol)['price'])
            return round(price * 0.01, 2)

        atr_series = talib.ATR(df["high"], df["low"], df["close"], timeperiod=14)
        latest_atr = atr_series.iloc[-1]

        if pd.isna(latest_atr) or latest_atr <= 0:
            logger.warning(f"[{symbol}] ⚠️ ATR invalide ({latest_atr}). Fallback.")
            price = float(client.ticker_price(symbol=symbol)['price'])
            return round(price * 0.01, 2)

        logger.debug(f"[{symbol}] ✅ ATR = {latest_atr:.2f}")
        return round(float(latest_atr), 2)

    except Exception as e:
        logger.error(f"[{symbol}] ❌ Erreur ATR: {e}")
        try:
            price = float(client.ticker_price(symbol=symbol)['price'])
            logger.warning(f"[{symbol}] ⚠️ Fallback ATR = 1% du prix actuel: {price}")
            return round(price * 0.01, 2)
        except Exception as e2:
            logger.critical(f"[{symbol}] ❌ Erreur critique lors du fallback ATR: {e2}")
            return None
