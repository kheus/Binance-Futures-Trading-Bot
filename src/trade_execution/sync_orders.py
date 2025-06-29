# sync_orders.py
from operator import ge
from src.trade_execution.order_utils import get_open_orders
from src.database.db_handler import sync_orders_with_db
import logging
import time

logger = logging.getLogger(__name__)

def sync_binance_orders_with_postgres(client, symbol_list):
    """Wrapper avec gestion des erreurs améliorée"""
    try:
        logger.info("Starting order sync process...")
        start_time = time.time()
        
        result = sync_orders_with_db(client, symbol_list)
        
        duration = time.time() - start_time
        logger.info(f"Sync completed in {duration:.2f}s. Result: {result}")
        return result
        
    except Exception as e:
        logger.error(f"Order sync failed completely: {str(e)}", exc_info=True)
        return 0
