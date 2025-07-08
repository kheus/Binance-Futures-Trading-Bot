# File: src/trade_execution/sync_orders.py

import logging
import time
from binance.um_futures import UMFutures
from src.monitoring.metrics import get_current_atr, get_current_adx
from src.trade_execution.ultra_aggressive_trailing import TrailingStopManager
from src.database.db_handler import insert_or_update_order, update_trade_on_close, insert_trade, execute_query
from src.trade_execution.order_manager import EnhancedOrderManager
from binance.exceptions import BinanceAPIException

logger = logging.getLogger(__name__)
_processed_orders = set()

def sync_binance_trades_with_postgres(client: UMFutures, symbols, ts_manager: TrailingStopManager, current_positions: dict):
    """Synchronisation robuste des ordres ouverts et des positions avec gestion d'erreur améliorée."""
    logger.info("[sync_orders] Starting optimized open orders sync")
    
    for symbol in symbols:
        try:
            # Log current state
            logger.debug(f"[sync_orders] Current positions before sync for {symbol}: {current_positions.get(symbol)}")
            
            # Vérifier les trades ouverts dans la base de données
            open_trades = execute_query(
                "SELECT trade_id, symbol, side, quantity, price FROM trades WHERE status IN ('new', 'OPEN') AND symbol = %s",
                params=(symbol,),
                fetch=True
            )
            
            # Vérifier les positions actives sur Binance
            try:
                positions = client.get_position_risk(symbol=symbol)
                if positions is None:
                    logger.warning(f"[sync_orders] No position data returned for {symbol}")
                    active_positions = {}
                else:
                    active_positions = {pos['symbol']: pos for pos in positions if float(pos['positionAmt']) != 0}
            except BinanceAPIException as e:
                logger.error(f"[sync_orders] Failed to fetch position risk for {symbol}: {e}")
                active_positions = {}
                continue
            
            # Vérifier si les trades ouverts dans la base de données existent toujours sur Binance
            for trade in open_trades:
                trade_id, trade_symbol, side, qty, entry_price = trade
                if trade_symbol not in active_positions:
                    # La position n'existe plus sur Binance, elle a été fermée
                    logger.info(f"[sync_orders] Trade {trade_id} for {trade_symbol} not found in active positions, marking as CLOSED")
                    try:
                        ticker = client.ticker_price(symbol=trade_symbol)
                        exit_price = float(ticker['price'])
                    except BinanceAPIException as e:
                        logger.error(f"[sync_orders] Failed to fetch ticker for {trade_symbol}: {e}")
                        exit_price = float(entry_price)  # Fallback to entry price
                    qty = float(qty)
                    if side.lower() == 'buy':
                        pnl = (exit_price - float(entry_price)) * qty
                    else:
                        pnl = (float(entry_price) - exit_price) * qty
                    execute_query(
                        "UPDATE trades SET status = 'CLOSED', exit_price = %s, realized_pnl = %s, close_timestamp = %s WHERE trade_id = %s",
                        params=(exit_price, pnl, int(time.time()), trade_id)
                    )
                    # Supprimer le trailing stop s'il existe
                    if ts_manager.has_trailing_stop(trade_symbol):
                        ts_manager.close_position(trade_symbol)
                        logger.info(f"[sync_orders] Trailing stop removed for closed trade {trade_id} on {trade_symbol}")
                    if current_positions.get(trade_symbol):
                        current_positions[trade_symbol] = None
                        logger.info(f"[sync_orders] Cleared current_positions for {trade_symbol}")
            
            # Check if an open position exists
            side = current_positions[symbol]['side'] if current_positions.get(symbol) else None
            try:
                order_manager = EnhancedOrderManager(client, symbols)
                has_position, position_qty = order_manager.check_open_position(symbol, side, current_positions)
                logger.debug(f"[sync_orders] Position check for {symbol}: has_position={has_position}, qty={position_qty}")
            except Exception as e:
                logger.error(f"[sync_orders] Failed to check position for {symbol}: {e}")
                has_position = False
                position_qty = 0.0

            # 1. Récupération des ordres ouverts avec gestion d'erreur renforcée
            try:
                all_orders = client.get_all_orders(symbol=symbol, limit=50)
                open_orders = [o for o in all_orders if o.get('status') in ['NEW', 'PARTIALLY_FILLED']]
                
                if not open_orders and not has_position and current_positions.get(symbol) is not None:
                    # Only clear position if no open orders, no position, and no trailing stop
                    if not ts_manager.has_trailing_stop(symbol):
                        logger.debug(f"[sync_orders] No open orders or position for {symbol}, clearing current_positions")
                        current_positions[symbol] = None
                    else:
                        logger.debug(f"[sync_orders] Trailing stop exists for {symbol}, preserving current_positions")
                    continue
                    
            except BinanceAPIException as e:
                logger.error(f"[sync_orders] API Error for {symbol}: {e}")
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
                        trades = client.get_account_trades(symbol=symbol, limit=10)
                        related_trades = [t for t in trades if str(t.get('orderId', '')).strip() == order_id]
                        
                        if not related_trades:
                            logger.debug(f"[sync_orders] No trades found for order {effective_id}")
                            continue
                            
                    except BinanceAPIException as e:
                        logger.error(f"[sync_orders] Trade fetch error: {e}")
                        continue

                    # 4. Traitement du premier trade trouvé
                    trade = related_trades[0]
                    trade_id = client_order_id or f"trade_{trade['time']}"
                    trade_id = trade_id.replace('trade_', '')  # Normalize trade_id
                    trade_data = {
                        'order_id': effective_id,
                        'symbol': symbol,
                        'side': order['side'].lower(),
                        'quantity': float(trade.get('qty', 0)),
                        'price': float(trade.get('price', 0)),
                        'timestamp': int(trade.get('time', time.time() * 1000)) // 1000,
                        'trade_id': trade_id,
                        'is_trailing': False,
                        'stop_loss': None,
                        'take_profit': None,
                        'pnl': 0.0
                    }

                    # 5. Insertion et gestion du stop
                    if insert_trade(trade_data):
                        current_positions[symbol] = {
                            'side': trade_data['side'],
                            'quantity': trade_data['quantity'],
                            'price': trade_data['price'],
                            'trade_id': trade_id
                        }
                        logger.debug(f"[sync_orders] Updated current_positions for {symbol}: {current_positions[symbol]}")
                        atr = get_current_atr(client, symbol)
                        adx = get_current_adx(client, symbol)

                        if atr and atr > 0 and adx and adx > 0:
                           ts_manager.initialize_trailing_stop(
                               symbol=symbol,
                               entry_price=trade_data['price'],
                               position_type='long' if order['side'].lower() == 'buy' else 'short',
                               quantity=trade_data['quantity'],
                               atr=atr,
                               adx=adx,
                               trade_id=trade_id
                           )
                           trade_data['is_trailing'] = True
                           insert_trade(trade_data)
                           logger.info(f"[sync_orders] Initialized trailing stop for {symbol}, trade_id={trade_id}")

                except Exception as e:
                    logger.error(f"[sync_orders] Processing error for order {effective_id}: {e}")
                    continue

        except Exception as e:
            logger.error(f"[sync_orders] Symbol processing error for {symbol}: {e}")
            continue

    logger.info("[sync_orders] Sync completed successfully")
    logger.debug(f"[sync_orders] Final current_positions: {current_positions}")