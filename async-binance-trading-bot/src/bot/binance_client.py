from binance import AsyncClient, BinanceSocketManager
import asyncio
import logging

class BinanceClient:
    def __init__(self, api_key, api_secret):
        self.api_key = api_key
        self.api_secret = api_secret
        self.client = None
        self.socket_manager = None

    async def initialize(self):
        try:
            self.client = await AsyncClient.create(self.api_key, self.api_secret)
            self.socket_manager = BinanceSocketManager(self.client)
            logging.info("Binance client initialized successfully.")
        except Exception as e:
            logging.error(f"Failed to initialize Binance client: {e}")
            raise

    async def close(self):
        if self.client:
            await self.client.close_connection()
            logging.info("Binance client connection closed.")

    async def get_symbol_ticker(self, symbol):
        try:
            ticker = await self.client.get_symbol_ticker(symbol=symbol)
            return ticker
        except Exception as e:
            logging.error(f"Error fetching ticker for {symbol}: {e}")
            return None

    async def place_order(self, symbol, side, order_type, quantity, price=None):
        try:
            if order_type == 'LIMIT':
                order = await self.client.order_limit(
                    symbol=symbol,
                    side=side,
                    quantity=quantity,
                    price=price
                )
            elif order_type == 'MARKET':
                order = await self.client.order_market(
                    symbol=symbol,
                    side=side,
                    quantity=quantity
                )
            logging.info(f"Order placed: {order}")
            return order
        except Exception as e:
            logging.error(f"Error placing order for {symbol}: {e}")
            return None

    async def get_account_info(self):
        try:
            account_info = await self.client.get_account()
            return account_info
        except Exception as e:
            logging.error(f"Error fetching account info: {e}")
            return None