#!/usr/bin/env python3
"""
Test Binance API connection and basic operations.
"""
import sys
import os
import logging
import time
from pathlib import Path
from datetime import datetime, timedelta

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_binance_connection():
    """Test Binance API connection and basic operations."""
    try:
        from binance.um_futures import UMFutures
        from binance.lib.utils import config_logging
        from src.utils.config import config
        
        # Configure logging for binance
        config_logging(logging, logging.INFO)
        
        # Initialize client
        client = UMFutures(
            key=config.binance.api_key,
            secret=config.binance.api_secret,
            base_url=config.binance.base_url
        )
        
        # Test connection
        logger.info("Testing Binance API connection...")
        server_time = client.time()
        server_time_str = datetime.fromtimestamp(server_time['serverTime'] / 1000).strftime('%Y-%m-%d %H:%M:%S')
        logger.info(f"Connected to Binance API. Server time: {server_time_str}")
        
        # Test market data
        symbols = ['BTCUSDT', 'ETHUSDT']
        for symbol in symbols:
            try:
                # Test order book
                depth = client.depth(symbol=symbol, limit=5)
                bid = float(depth['bids'][0][0]) if depth['bids'] else 0
                ask = float(depth['asks'][0][0]) if depth['asks'] else 0
                spread = ask - bid if bid and ask else 0
                logger.info(f"{symbol} Order Book - Bid: {bid:.2f}, Ask: {ask:.2f}, Spread: {spread:.4f}")
                
                # Test klines
                end_time = int(time.time() * 1000)
                start_time = end_time - 86400000  # 24 hours ago
                klines = client.klines(
                    symbol=symbol,
                    interval='1h',
                    startTime=start_time,
                    endTime=end_time,
                    limit=1
                )
                
                if klines:
                    kline = klines[0]
                    logger.info(f"{symbol} Latest Kline:")
                    logger.info(f"  Open Time: {datetime.fromtimestamp(kline[0]/1000)}")
                    logger.info(f"  Open: {kline[1]}")
                    logger.info(f"  High: {kline[2]}")
                    logger.info(f"  Low: {kline[3]}")
                    logger.info(f"  Close: {kline[4]}")
                    logger.info(f"  Volume: {kline[5]}")
                
                # Test account info (if API keys are provided)
                if config.binance.api_key and config.binance.api_secret:
                    try:
                        account = client.account()
                        logger.info(f"Account status: {account.get('canTrade', 'Unknown')}")
                        
                        # Get positions
                        positions = [p for p in client.get_position_risk() if float(p['positionAmt']) != 0]
                        if positions:
                            logger.info("Open positions:")
                            for pos in positions:
                                logger.info(f"  {pos['symbol']}: {pos['positionAmt']} @ {pos['entryPrice']} (P&L: {pos['unRealizedProfit']} USDT)")
                        else:
                            logger.info("No open positions")
                    except Exception as e:
                        logger.warning(f"Could not fetch account info: {e}")
                
            except Exception as e:
                logger.error(f"Error testing {symbol}: {e}")
                continue
        
        return True
        
    except Exception as e:
        logger.error(f"Binance API test failed: {e}", exc_info=True)
        return False

if __name__ == "__main__":
    print("Testing Binance API connection...")
    success = test_binance_connection()
    if success:
        print("✅ Binance API connection test passed!")
        sys.exit(0)
    else:
        print("❌ Binance API connection test failed!")
        sys.exit(1)
