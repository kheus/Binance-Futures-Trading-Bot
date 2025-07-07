import asyncio
import signal
import logging
from src.bot.binance_client import initialize_binance_client
from src.bot.order_manager import OrderManager
from src.bot.strategy import TradingStrategy
from src.bot.shutdown import handle_shutdown

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def main_logic():
    client = initialize_binance_client()
    order_manager = OrderManager(client)
    strategy = TradingStrategy(client)

    try:
        while True:
            await strategy.analyze_market()
            await order_manager.manage_orders()
            await asyncio.sleep(1)  # Adjust the sleep time as needed
    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        await order_manager.close()

def main():
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: handle_shutdown(loop))
    try:
        loop.run_until_complete(main_logic())
    except (KeyboardInterrupt, SystemExit):
        handle_shutdown(loop)

if __name__ == "__main__":
    main()