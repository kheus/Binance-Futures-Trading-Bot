import logging
import time
import decimal
import pandas as pd
import numpy as np
from src.database.db_handler import get_pending_training_data, update_training_outcome, clean_old_data, execute_query
from tabulate import tabulate
import psycopg2.pool
import eventlet
import datetime

logger = logging.getLogger(__name__)

def safe_float(val):
    """Safely convert value to float, handling Decimals, None, and invalid values"""
    if val is None:
        return None
    try:
        if isinstance(val, decimal.Decimal):
            return float(val)
        return float(val)
    except (TypeError, ValueError):
        return None

def calculate_market_direction(symbol, signal_ts):
    """Calculate market direction with proper timestamp handling"""
    try:
        future_ts = signal_ts + 5 * 60 * 1000  # 5 minutes after
        window_end_ts = signal_ts + 30 * 60 * 1000  # 30-minute window
        
        # Query with direct BIGINT comparison
        query = """
        SELECT close, timestamp 
        FROM price_data 
        WHERE symbol = %s 
        AND timestamp >= %s
        AND timestamp <= %s
        ORDER BY timestamp ASC 
        LIMIT 1
        """
        result = execute_query(query, (symbol, int(future_ts), int(window_end_ts)), fetch=True)
        
        if result:
            future_price = safe_float(result[0][0])
            future_ts_found = result[0][1]
            
            # Get signal price with direct BIGINT comparison
            query = """
            SELECT price FROM signals 
            WHERE symbol = %s AND timestamp = %s
            """
            signal_result = execute_query(query, (symbol, int(signal_ts)), fetch=True)
            
            if signal_result and (signal_price := safe_float(signal_result[0][0])):
                pct_change = ((future_price - signal_price) / signal_price) * 100
                direction = 1 if future_price > signal_price else 0
                logger.info(f"[Market] Direction for {symbol}: {direction} ({pct_change:.2f}%)")
                return direction, pct_change
                
        # Fallback to most recent price
        query = """
        SELECT close 
        FROM price_data 
        WHERE symbol = %s 
        AND timestamp <= %s
        ORDER BY timestamp DESC 
        LIMIT 1
        """
        recent_result = execute_query(query, (symbol, int(window_end_ts)), fetch=True)
        
        if recent_result and (recent_price := safe_float(recent_result[0][0])):
            query = """
            SELECT price FROM signals 
            WHERE symbol = %s AND timestamp = %s
            """
            signal_result = execute_query(query, (symbol, int(signal_ts)), fetch=True)
            
            if signal_result and (signal_price := safe_float(signal_result[0][0])):
                pct_change = ((recent_price - signal_price) / signal_price) * 100
                direction = 1 if recent_price > signal_price else 0
                logger.info(f"[Market] Used recent price for {symbol}: {direction}")
                return direction, pct_change
                
        logger.warning(f"[Market] No price data for {symbol} (window: {future_ts} to {window_end_ts})")
        return None, None
        
    except Exception as e:
        logger.error(f"[Market] Error for {symbol}: {str(e)}", exc_info=True)
        return None, None

def performance_tracker_loop(client, symbols):
    """Main tracking loop with robust error handling"""
    while True:
        try:
            for symbol in symbols:
                # Get pending records
                query = """
                SELECT id, symbol, timestamp, action, price
                FROM training_data
                WHERE symbol = %s AND market_direction IS NULL
                ORDER BY timestamp ASC
                LIMIT 100
                """
                records = execute_query(query, (symbol,), fetch=True)
                
                if not records:
                    logger.debug(f"[Tracker] No pending records for {symbol}")
                    continue

                processed = 0
                for record in records:
                    record_id, symbol, timestamp, action, price = record
                    
                    # Ensure numeric types
                    price = safe_float(price)
                    timestamp = int(timestamp) if timestamp else None
                    
                    if not action:
                        action = get_action_from_signals(symbol, timestamp)
                        if not action:
                            logger.warning(f"[Tracker] No signal found for {symbol} at {timestamp}, using default 'hold'")
                            action = "hold"
                    
                    if price is None:
                        price = get_price_for_timestamp(symbol, timestamp)
                        if price is None:
                            continue

                    # Get future price with direct BIGINT comparison
                    future_ts = timestamp + 5 * 60 * 1000
                    query = """
                    SELECT close
                    FROM price_data
                    WHERE symbol = %s 
                    AND timestamp >= %s
                    AND timestamp <= %s
                    ORDER BY timestamp ASC
                    LIMIT 1
                    """
                    price_data = execute_query(query, (symbol, int(future_ts), int(future_ts + 360 * 1000)), fetch=True)
                    
                    if not price_data or (future_price := safe_float(price_data[0][0])) is None:
                        logger.debug(f"[Tracker] No future price for {symbol} at {timestamp}")
                        continue

                    # Calculate metrics
                    price_change = (future_price - price) / price * 100
                    market_direction = 1 if future_price > price else 0
                    prediction_correct = calculate_prediction_accuracy(action, market_direction)

                    # Update record
                    update_query = """
                    UPDATE training_data
                    SET market_direction = %s, 
                        price_change_pct = %s, 
                        prediction_correct = %s, 
                        updated_at = %s,
                        action = %s,
                        price = %s
                    WHERE id = %s
                    """
                    params = (
                        market_direction,
                        price_change,
                        prediction_correct,
                        int(time.time() * 1000),
                        action,
                        price,
                        record_id
                    )
                    execute_query(update_query, params, fetch=False)
                    processed += 1

                log_processing_results(symbol, processed)
                
            time.sleep(60)
            
        except Exception as e:
            logger.error(f"[Tracker] Loop error: {str(e)}", exc_info=True)
            time.sleep(60)

# Helper functions

def get_action_from_signals(symbol, timestamp):
    """Fetch the action from the signals table for a given symbol and timestamp."""
    query = """
    SELECT signal_type FROM signals
    WHERE symbol = %s AND timestamp = %s
    LIMIT 1
    """
    result = execute_query(query, (symbol, int(timestamp)), fetch=True)
    return result[0][0] if result else None

def get_price_for_timestamp(symbol, timestamp):
    """Get price for a symbol at specific timestamp"""
    query = """
    SELECT close FROM price_data 
    WHERE symbol = %s AND timestamp <= %s
    ORDER BY timestamp DESC LIMIT 1
    """
    result = execute_query(query, (symbol, int(timestamp)), fetch=True)
    return safe_float(result[0][0]) if result else None

def calculate_prediction_accuracy(action, market_direction):
    """Determine if prediction was correct"""
    if action == "hold":
        return None
    return (
        (action in ['buy', 'close_sell'] and market_direction == 1) or
        (action in ['sell', 'close_buy'] and market_direction == 0)
    )

def log_processing_results(symbol, processed):
    """Log processing results with accuracy metrics"""
    if processed > 0:
        query = """
        SELECT COUNT(*) FILTER (WHERE prediction_correct = TRUE)::FLOAT / 
               NULLIF(COUNT(*), 0) * 100 AS accuracy
        FROM training_data
        WHERE symbol = %s AND prediction_correct IS NOT NULL
        """
        accuracy_result = execute_query(query, (symbol,), fetch=True)
        accuracy = accuracy_result[0][0] if accuracy_result else 0.0
        logger.info(f"[Tracker] Processed {processed} {symbol} records | Accuracy: {accuracy:.2f}%")

if __name__ == "__main__":
    logger.info("Starting performance tracker")
    performance_tracker_loop(None, ["BTCUSDT", "ETHUSDT"])  # Example symbols