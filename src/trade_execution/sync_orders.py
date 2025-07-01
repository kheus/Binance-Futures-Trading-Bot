import logging
from binance.um_futures import UMFutures
from src.monitoring.metrics import get_current_atr
from src.trade_execution.ultra_aggressive_trailing import TrailingStopManager
from src.database.db_handler import insert_or_update_order

logger = logging.getLogger(__name__)

def sync_binance_trades_with_postgres(client: UMFutures, symbols, ts_manager):
    logger.info("[sync_orders] Syncing Binance trades with PostgreSQL and internal tracker...")
    for symbol in symbols:
        try:
            # Récupérer tous les ordres (ouverts, partiellement remplis, exécutés)
            all_orders = client.get_all_orders(symbol=symbol, limit=100)
            logger.debug(f"Raw all_orders response for {symbol}: {all_orders}")
            if not isinstance(all_orders, list):
                logger.warning(f"Invalid response format for {symbol}: {all_orders}")
                continue
            if not all_orders:
                logger.warning(f"No orders received for {symbol}")
                continue
            
            for order in all_orders:
                if not isinstance(order, dict) or not order.get('orderId'):
                    logger.warning(f"Skipping invalid order for {symbol}: {order}")
                    continue
                order_id = str(order['orderId'])  # Conversion explicite en chaîne
                insert_or_update_order(order)
                logger.info(f"Order {order_id} for {symbol} inserted/updated in DB.")

                # Récupérer les trades exécutés uniquement pour les ordres remplis
                if order['status'] == 'FILLED':
                    trades = client.get_account_trades(symbol=symbol, limit=100)
                    for trade in trades:
                        if str(trade.get('orderId')) == order_id:
                            entry_price = float(trade.get('price', 0.0))
                            if entry_price <= 0:
                                logger.warning(f"Invalid entry_price from trade for {symbol}: {entry_price}, using current price")
                                ticker = client.ticker_price(symbol=symbol)
                                entry_price = float(ticker['price'])
                            quantity = float(trade['qty'])
                            position_type = "long" if order['side'] == 'BUY' else "short"
                            atr = get_current_atr(client, symbol)
                            ts_manager.initialize_trailing_stop(symbol, entry_price, position_type, quantity, atr)
                            logger.info(f"Initialized trailing stop for recovered trade {order_id} ({symbol})")
        except Exception as e:
            logger.error(f"[sync_orders] Error syncing trades for {symbol}: {str(e)}")
            continue