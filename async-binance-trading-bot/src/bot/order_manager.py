from binance.um_futures import UMFutures
import logging

class OrderManager:
    def __init__(self, client):
        self.client = client

    async def place_order(self, symbol, side, quantity, order_type='MARKET'):
        try:
            order = await self.client.new_order(
                symbol=symbol,
                side=side,
                type=order_type,
                quantity=quantity
            )
            logging.info(f"Order placed: {order}")
            return order
        except Exception as e:
            logging.error(f"Error placing order: {e}")
            return None

    async def cancel_order(self, symbol, order_id):
        try:
            result = await self.client.cancel_order(symbol=symbol, orderId=order_id)
            logging.info(f"Order canceled: {result}")
            return result
        except Exception as e:
            logging.error(f"Error canceling order: {e}")
            return None

    async def modify_order(self, symbol, order_id, new_quantity):
        try:
            order = await self.client.get_order(symbol=symbol, orderId=order_id)
            if order:
                new_order = await self.place_order(symbol, order['side'], new_quantity, order['type'])
                logging.info(f"Order modified: {new_order}")
                return new_order
            else:
                logging.warning("Order not found for modification.")
                return None
        except Exception as e:
            logging.error(f"Error modifying order: {e}")
            return None