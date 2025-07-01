import logging
from binance.um_futures import UMFutures
from src.monitoring.metrics import get_current_atr
from src.trade_execution.ultra_aggressive_trailing import TrailingStopManager
from src.database.db_handler import insert_or_update_order
from src.trade_execution.order_manager import check_open_position
import time

logger = logging.getLogger(__name__)

def sync_binance_trades_with_postgres(client: UMFutures, symbols, ts_manager):
    logger.info("[sync_orders] Syncing Binance trades with PostgreSQL and internal tracker...")
    # Filtrer les ordres des dernières 24 heures (en millisecondes)
    start_time = int((time.time() - 24 * 60 * 60) * 1000)
    for symbol in symbols:
        try:
            # Récupérer tous les ordres récents
            all_orders = client.get_all_orders(symbol=symbol, limit=100, startTime=start_time)
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
                order_id = str(order['orderId'])
                insert_or_update_order(order)

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
                            trade_id = order.get('clientOrderId', str(int(trade['time'])))
                            
                            # Vérifier si une position est ouverte
                            has_position, position_qty = check_open_position(client, symbol, order['side'])
                            if not has_position:
                                logger.info(f"[sync_orders] No open position for {symbol} on side {order['side']}. Skipping trailing stop.")
                                continue

                            # Vérifier si un trailing stop existe déjà
                            open_orders = client.get_open_orders(symbol=symbol)
                            for open_order in open_orders:
                                if open_order['clientOrderId'].startswith(f"trailing_stop_{symbol}_{trade_id}"):
                                    logger.info(f"[sync_orders] Trailing stop already exists for {symbol} trade {trade_id}. Skipping.")
                                    continue

                            atr = get_current_atr(client, symbol)
                            ts_manager.initialize_trailing_stop(
                                symbol=symbol,
                                entry_price=entry_price,
                                position_type=position_type,
                                quantity=quantity,
                                atr=atr,
                                trade_id=trade_id
                            )
                            logger.info(f"Initialized trailing stop for recovered trade {order_id} ({symbol})")
        except Exception as e:
            logger.error(f"[sync_orders] Error syncing trades for {symbol}: {str(e)}")
            continue