# File: src/trade_execution/sync_orders.py
import logging
from binance.um_futures import UMFutures
from src.monitoring.metrics import get_current_atr
from src.trade_execution.ultra_aggressive_trailing import TrailingStopManager
from src.database.db_handler import insert_or_update_order, update_trade_on_close, insert_trade
from src.trade_execution.order_manager import check_open_position
import time
import eventlet

logger = logging.getLogger(__name__)

def sync_binance_trades_with_postgres(client: UMFutures, symbols, ts_manager: TrailingStopManager, current_positions: dict):
    logger.info("[sync_orders] Syncing Binance trades with PostgreSQL and internal tracker... 🚀")
    start_time = int((time.time() - 24 * 60 * 60) * 1000)  # Last 24 hours
    no_position_symbols = set()
    for symbol in symbols:
        try:
            all_orders = client.get_all_orders(symbol=symbol, limit=20, startTime=start_time)
            if not isinstance(all_orders, list):
                logger.warning(f"⚠️ Invalid response format for {symbol}: {all_orders}")
                continue
            if not all_orders:
                logger.info(f"[sync_orders] No orders received for {symbol}")
                continue
            for order in all_orders:
                logger.debug(f"[sync_orders] Processing order for {symbol}: {order}")
                if not isinstance(order, dict):
                    logger.warning(f"⚠️ Skipping non-dict order for {symbol}: {order}")
                    continue
                if not order.get('orderId'):
                    logger.error(f"❌ Skipped order with missing orderId for {symbol}: {order}")
                    continue
                try:
                    order_id = str(order['orderId'])
                    insert_or_update_order(order)
                    if order['status'] == 'FILLED':
                        trades = client.get_account_trades(symbol=symbol, limit=100)
                        logger.debug(f"[sync_orders] Retrieved trades for {symbol}: {trades}")
                        for trade in trades:
                            if not isinstance(trade, dict):
                                logger.warning(f"⚠️ Skipping non-dict trade for {symbol}: {trade}")
                                continue
                            if not trade.get('orderId'):
                                logger.error(f"❌ Skipped trade with missing orderId for {symbol}: {trade}")
                                continue
                            if str(trade.get('orderId')) == order_id:
                                entry_price = float(trade.get('price', 0.0))
                                if entry_price <= 0:
                                    logger.warning(f"⚠️ Invalid entry_price from trade for {symbol}: {entry_price}, using current price")
                                    try:
                                        ticker = client.ticker_price(symbol=symbol)
                                        entry_price = float(ticker.get('price', 0.0))
                                    except Exception as e:
                                        logger.error(f"❌ Failed to fetch ticker price for {symbol}: {str(e)}")
                                        continue
                                quantity = float(trade.get('qty', 0.0))
                                position_type = "long" if order['side'] == 'BUY' else "short"
                                trade_id = order.get('clientOrderId', str(int(trade['time'])))
                                trade_data = {
                                    'order_id': order_id,
                                    'symbol': symbol,
                                    'side': order['side'],
                                    'quantity': quantity,
                                    'price': entry_price,
                                    'exit_price': None,
                                    'stop_loss': None,
                                    'take_profit': None,
                                    'timestamp': trade.get('time') / 1000,
                                    'pnl': 0.0,
                                    'is_trailing': False,
                                    'trade_id': trade_id
                                }
                                logger.debug(f"[sync_orders] Inserting trade for {symbol}, order_id: {order_id}, trade_id: {trade_id}")
                                success = insert_trade(trade_data)
                                if not success:
                                    logger.error(f"[sync_orders] Failed to insert trade for {symbol}, order_id: {order_id}")
                                    continue
                                has_position, position_qty = check_open_position(client, symbol, order['side'], current_positions)
                                if not has_position:
                                    no_position_symbols.add(symbol)
                                    trade_orders = client.get_account_trades(symbol=symbol, limit=100)
                                    for trade_order in trade_orders:
                                        if (trade_order.get('orderId') != order['orderId'] and 
                                            trade_order.get('side') != order['side'] and 
                                            float(trade_order.get('qty', 0.0)) == quantity):
                                            exit_price = float(trade_order.get('price', 0.0))
                                            success = update_trade_on_close(symbol, order_id, exit_price, quantity, order['side'])
                                            if success:
                                                logger.info(f"[sync_orders] Updated closed trade for {symbol}, order_id: {order_id}, exit_price: {exit_price}")
                                            continue
                                open_orders = client.get_open_orders(symbol=symbol)
                                for open_order in open_orders:
                                    if not isinstance(open_order, dict) or not open_order.get('clientOrderId'):
                                        logger.warning(f"⚠️ Skipping invalid open order for {symbol}: {open_order}")
                                        continue
                                    if open_order['clientOrderId'].startswith(f"trailing_stop_{symbol}_{trade_id}"):
                                        logger.debug(f"[sync_orders] Trailing stop already exists for {symbol} trade {trade_id}. Skipping.")
                                        continue
                                atr = get_current_atr(client, symbol)
                                if atr <= 0:
                                    logger.error(f"❌ [sync_orders] Invalid ATR for {symbol}: {atr}")
                                    continue
                                ts_manager.initialize_trailing_stop(
                                    symbol=symbol,
                                    entry_price=entry_price,
                                    position_type=position_type,
                                    quantity=quantity,
                                    atr=atr,
                                    trade_id=trade_id
                                )
                                logger.info(f"[sync_orders] Initialized trailing stop for recovered trade {order_id} ({symbol}) 📈")
                except Exception as e:
                    order_id = order.get('orderId', order.get('clientOrderId', 'unknown')) if isinstance(order, dict) else 'invalid'
                    logger.error(f"❌ [sync_orders] Error processing order {order_id} for {symbol}: {str(e)}")
                    continue
            eventlet.sleep(0.1)
        except Exception as e:
            logger.error(f"❌ [sync_orders] Error syncing trades for {symbol}: {str(e)}")
            continue
    if no_position_symbols:
        logger.info(f"[sync_orders] No open positions found for symbols: {', '.join(no_position_symbols)}")
    logger.info("[sync_orders] Trade sync completed. ✅")