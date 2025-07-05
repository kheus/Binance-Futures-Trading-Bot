# File: src/trade_execution/sync_orders.py

import logging
import time
import eventlet
from binance.um_futures import UMFutures
from src.monitoring.metrics import get_current_atr
from src.trade_execution.ultra_aggressive_trailing import TrailingStopManager
from src.database.db_handler import insert_or_update_order, update_trade_on_close, insert_trade
from src.trade_execution.order_manager import check_open_position

logger = logging.getLogger(__name__)
_processed_orders = set()

def sync_binance_trades_with_postgres(client: UMFutures, symbols, ts_manager: TrailingStopManager, current_positions: dict):
    """Synchronisation robuste des ordres ouverts avec gestion d'erreur améliorée"""
    logger.info("[sync_orders] Starting optimized open orders sync")
    
    for symbol in symbols:
        try:
            # 1. Récupération des ordres ouverts avec gestion d'erreur renforcée
            try:
                # Solution: Utiliser get_all_orders avec filter manuel
                all_orders = client.get_all_orders(symbol=symbol, limit=50)
                open_orders = [o for o in all_orders if o.get('status') in ['NEW', 'PARTIALLY_FILLED']]
                
                if not open_orders:
                    logger.debug(f"[sync_orders] No open orders for {symbol}")
                    continue
                    
            except Exception as e:
                logger.error(f"[sync_orders] API Error for {symbol}: {str(e)}")
                continue

            # 2. Traitement des ordres
            for order in open_orders:
                try:
                    # Validation robuste de l'orderId
                    order_id = str(order.get('orderId', '')).strip()
                    client_order_id = str(order.get('clientOrderId', '')).strip()
                    
                    if not order_id and not client_order_id:
                        logger.warning(f"[sync_orders] Invalid order - no ID for {symbol}")
                        continue
                        
                    effective_id = order_id or client_order_id
                    
                    # Vérification des doublons
                    if effective_id in _processed_orders:
                        continue
                    _processed_orders.add(effective_id)

                    # Enregistrement de l'ordre
                    if not insert_or_update_order(order):
                        logger.error(f"[sync_orders] Failed to save order {effective_id}")
                        continue

                    # 3. Récupération des trades associés
                    try:
                        # Solution alternative pour éviter l'erreur orderId
                        trades = client.get_account_trades(symbol=symbol, limit=10)
                        related_trades = [t for t in trades if str(t.get('orderId', '')).strip() == order_id]
                        
                        if not related_trades:
                            logger.debug(f"[sync_orders] No trades found for order {effective_id}")
                            continue
                            
                    except Exception as e:
                        logger.error(f"[sync_orders] Trade fetch error: {str(e)}")
                        continue

                    # 4. Traitement du premier trade trouvé
                    trade = related_trades[0]
                    trade_data = {
                        'order_id': effective_id,
                        'symbol': symbol,
                        'side': order['side'],
                        'quantity': float(trade.get('qty', 0)),
                        'price': float(trade.get('price', 0)),
                        'timestamp': int(trade.get('time', time.time() * 1000)) // 1000,
                        'trade_id': client_order_id or f"trade_{trade['time']}",
                        'is_trailing': False
                    }

                    # 5. Insertion et gestion du stop
                    if insert_trade(trade_data):
                        atr = get_current_atr(client, symbol)
                        if atr and atr > 0:
                            ts_manager.initialize_trailing_stop(
                                symbol=symbol,
                                entry_price=trade_data['price'],
                                position_type='long' if order['side'] == 'BUY' else 'short',
                                quantity=trade_data['quantity'],
                                atr=atr,
                                trade_id=trade_data['trade_id']
                            )

                except Exception as e:
                    logger.error(f"[sync_orders] Processing error: {str(e)}")
                    continue

        except Exception as e:
            logger.error(f"[sync_orders] Symbol processing error: {str(e)}")
            continue

    logger.info("[sync_orders] Sync completed successfully")