# src/trade_execution/order_utils.py
import logging
from binance.um_futures import UMFutures
from pathlib import Path
import yaml

# Charge la configuration
CONFIG_PATH = Path(__file__).parent.parent.parent / "config" / "config.yaml"
with open(CONFIG_PATH) as f:
    config = yaml.safe_load(f)
SYMBOLS = config["binance"]["symbols"]

logger = logging.getLogger(__name__)

def get_open_orders(client: UMFutures, symbol: str = None) -> list:
    """Version corrigée pour l'API Binance Futures"""
    try:
        if symbol:
            # Pour un symbole spécifique
            return client.get_all_orders(symbol=symbol, limit=50)  # Note: get_all_orders au lieu de get_open_orders
        else:
            # Pour tous les symboles
            all_orders = []
            for sym in SYMBOLS:
                try:
                    orders = client.get_all_orders(symbol=sym, limit=50)
                    all_orders.extend([o for o in orders if o['status'] in ['NEW', 'PARTIALLY_FILLED']])
                except Exception as e:
                    logger.warning(f"Failed to get orders for {sym}: {str(e)}")
            return all_orders
    except Exception as e:
        logger.error(f"Order fetch error: {str(e)}", exc_info=True)
        return []