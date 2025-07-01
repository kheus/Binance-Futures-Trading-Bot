import logging
from binance.um_futures import UMFutures
from src.database.db_handler import DBHandler
from src.trade_execution.ultra_aggressive_trailing import init_trailing_stop_manager

logger = logging.getLogger(__name__)

class OrderSynchronizer:
    def __init__(self, client: UMFutures, db_handler: DBHandler, trailing_stop_manager):
        self.client = client
        self.db_handler = db_handler
        self.trailing_stop_manager = trailing_stop_manager

    def sync_orders(self):
        try:
            # Récupérer les positions ouvertes
            positions = self.client.get_position_risk()
            active_symbols = {pos['symbol'] for pos in positions if float(pos['positionAmt']) != 0}

            # Récupérer les ordres récents depuis Binance
            orders = self.client.get_all_orders(limit=100)
            for order in orders:
                symbol = order['symbol']
                order_id = order['orderId']
                status = order['status']

                # Insérer ou mettre à jour l'ordre dans la base de données
                self.db_handler.insert_or_update_order(
                    order_id=order_id,
                    symbol=symbol,
                    side=order['side'],
                    order_type=order['type'],
                    quantity=order['origQty'],
                    price=order.get('price', 0),
                    status=status,
                    timestamp=order['updateTime']
                )
                logger.info(f"[src.trade_execution.sync_orders] Order {order_id} for {symbol} inserted/updated in DB.")

                # Initialiser un trailing stop uniquement pour les ordres FILLED avec une position ouverte
                if status == 'FILLED' and symbol in active_symbols:
                    trades = self.client.get_account_trades(symbol=symbol, orderId=order_id)
                    for trade in trades:
                        if float(trade['qty']) > 0:
                            position_type = 'long' if order['side'] == 'BUY' else 'short'
                            atr = self.db_handler.get_latest_atr(symbol) or 0.01  # Fallback ATR
                            order_id = self.trailing_stop_manager.initialize_trailing_stop(
                                symbol=symbol,
                                entry_price=float(trade['price']),
                                position_type=position_type,
                                quantity=float(trade['qty']),
                                atr=atr
                            )
                            if order_id:
                                logger.info(f"[src.trade_execution.sync_orders] Initialized trailing stop for recovered trade {trade['id']} ({symbol})")
        except Exception as e:
            logger.error(f"[src.trade_execution.sync_orders] Failed to sync orders: {e}")

def init_order_synchronizer(client: UMFutures, db_handler: DBHandler):
    trailing_stop_manager = init_trailing_stop_manager(client)
    return OrderSynchronizer(client, db_handler, trailing_stop_manager)