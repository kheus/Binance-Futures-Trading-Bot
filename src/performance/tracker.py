import logging
import time
import pandas as pd
import numpy as np
from src.database.db_handler import get_pending_training_data, update_training_outcome, clean_old_data, execute_query
from tabulate import tabulate
import psycopg2.pool
import eventlet
import datetime

logger = logging.getLogger(__name__)

def calculate_market_direction(symbol, signal_ts):
    """
    Calculate market direction and percentage change after 5 minutes.
    """
    try:
        future_ts = signal_ts + 5 * 60 * 1000  # 5 minutes after
        window_end_ts = signal_ts + 15 * 60 * 1000  # Extended to 15-minute window
        query = """
        SELECT close, timestamp FROM price_data 
        WHERE symbol = %s 
        AND timestamp >= to_timestamp(%s::double precision / 1000) 
        AND timestamp <= to_timestamp(%s::double precision / 1000)
        ORDER BY timestamp ASC LIMIT 1
        """
        # Execute query to get future price
        result = execute_query(query, (symbol, future_ts, window_end_ts), fetch=True)
        if result:
            future_price, future_ts_found = float(result[0][0]), result[0][1]
            query = """
            SELECT price FROM signals 
            WHERE symbol = %s AND timestamp = %s
            """
            signal_result = execute_query(query, (symbol, signal_ts), fetch=True)
            if signal_result:
                signal_price = float(signal_result[0][0])
                pct_change = ((future_price - signal_price) / signal_price) * 100
                direction = 1 if future_price > signal_price else 0
                logger.info(f"[Market Direction] Found price for {symbol} at {future_ts_found}: {future_price}")
                return direction, pct_change

        # Log recent prices for debugging
        query = """
        SELECT timestamp, close 
        FROM price_data 
        WHERE symbol = %s 
        ORDER BY timestamp DESC 
        LIMIT 5
        """
        recent_prices = execute_query(query, (symbol,), fetch=True)
        logger.debug(f"[Market Direction] Recent prices for {symbol}: {recent_prices}")

        # Only warn if the window is in the past
        now_ts = int(time.time() * 1000)
        if future_ts > now_ts:
            logger.debug(f"[Market Direction] Skipped log for {symbol}: waiting for future data at {future_ts}")
        else:
            logger.warning(f"[Market Direction] No future price data for {symbol} at {future_ts} (window: {future_ts} to {window_end_ts})")
        return None, None
    except psycopg2.pool.PoolError as e:
        logger.error(f"[Market Direction] Connection pool exhausted for {symbol} at {signal_ts}: {str(e)}")
        return None, None
    except Exception as e:
        logger.error(f"[Market Direction] Error for {symbol} at {signal_ts}: {str(e)}")
        return None, None

def get_action_from_signals(symbol, signal_ts):
    """
    Fetch action (signal_type) from signals table for the given symbol and timestamp.
    """
    try:
        query = """
        SELECT signal_type FROM signals 
        WHERE symbol = %s AND timestamp = %s
        """
        result = execute_query(query, (symbol, signal_ts), fetch=True)
        if result:
            logger.debug(f"[Tracker] Found action {result[0][0]} for {symbol} at {signal_ts}")
            return result[0][0]  # Tuple access
        logger.debug(f"[Tracker] No signal found for {symbol} at {signal_ts}")
        return None
    except Exception as e:
        logger.error(f"[Tracker] Error fetching action for {symbol} at {signal_ts}: {str(e)}")
        return None

def should_retrain_model():
    """
    Determine if the model should be retrained based on recent performance.
    """
    try:
        query = """
        SELECT AVG(CAST(prediction_correct AS INTEGER)) as accuracy 
        FROM training_data 
        WHERE updated_at >= %s
        """
        result = execute_query(query, (int(time.time() * 1000) - 24 * 3600 * 1000,), fetch=True)
        accuracy = result[0][0] if result and result[0][0] is not None else 0
        logger.info(f"[Tracker] Model accuracy over last 24 hours: {accuracy:.2%}")
        return accuracy < 0.6  # Retrain if accuracy < 60%
    except Exception as e:
        logger.error(f"[Tracker] Error checking retrain condition: {str(e)}")
        return False

def performance_tracker_loop(client, symbols):
    while True:
        try:
            for symbol in symbols:
                query = """
                SELECT id, symbol, timestamp, action, price
                FROM training_data
                WHERE symbol = %s AND action IS NOT NULL AND market_direction IS NULL
                ORDER BY timestamp ASC
                LIMIT 100
                """
                records = execute_query(query, (symbol,), fetch=True)
                if not records:
                    logger.debug(f"[Tracker] No actionable records for {symbol}")
                    continue

                processed = 0
                for record in records:
                    record_id, symbol, timestamp, action, price = record
                    future_timestamp = timestamp + 5 * 60 * 1000
                    query = """
                    SELECT close
                    FROM price_data
                    WHERE symbol = %s AND timestamp >= %s AND timestamp <= %s
                    ORDER BY timestamp ASC
                    LIMIT 1
                    """
                    price_data = execute_query(query, (symbol, timestamp, future_timestamp + 360 * 1000), fetch=True)  # 6-minute window
                    if not price_data:
                        logger.warning(f"[Market Direction] No future price data for {symbol} at {timestamp} (window: {timestamp} to {future_timestamp + 360 * 1000})")
                        continue

                    future_price = float(price_data[0][0])
                    price_change = (future_price - price) / price * 100
                    market_direction = 'up' if future_price > price else 'down'
                    prediction_correct = (
                        (action in ['buy', 'close_sell'] and market_direction == 'up') or
                        (action in ['sell', 'close_buy'] and market_direction == 'down')
                    )

                    update_query = """
                    UPDATE training_data
                    SET market_direction = %s, price_change_pct = %s, prediction_correct = %s, updated_at = %s
                    WHERE id = %s
                    """
                    execute_query(update_query, (market_direction, price_change, prediction_correct, int(datetime.now().timestamp() * 1000), record_id), fetch=False)
                    processed += 1

                if processed > 0:
                    logger.info(f"[Tracker] Processed {processed} records for {symbol}")
                    accuracy_query = """
                    SELECT COUNT(*) FILTER (WHERE prediction_correct = TRUE) / NULLIF(COUNT(*), 0)::FLOAT * 100 AS accuracy
                    FROM training_data
                    WHERE symbol = %s AND prediction_correct IS NOT NULL
                    """
                    accuracy_result = execute_query(accuracy_query, (symbol,), fetch=True)
                    accuracy = accuracy_result[0][0] if accuracy_result and accuracy_result[0][0] is not None else 0.0
                    logger.info(f"[Tracker] Model accuracy for {symbol}: {accuracy:.2f}%")
                else:
                    logger.debug(f"[Tracker] No records processed for {symbol}")

            time.sleep(60)
        except Exception as e:
            logger.error(f"[Tracker] Error in performance tracker loop: {str(e)}")
            time.sleep(60)

def log_performance_as_table(record_id, symbol, accuracy, processed_records):
    """
    Log performance metrics as a table.
    """
    table_data = [
        ["Record ID", record_id],
        ["Symbol", symbol],
        ["Accuracy", f"{accuracy:.2%}"],
        ["Processed Records", processed_records]
    ]
    logger.info("Performance Tracker:\n%s", tabulate(table_data, headers=["Metric", "Value"], tablefmt="grid"))

if __name__ == "__main__":
    logger.info("Starting [Tracker] performance tracker")
    performance_tracker_loop(None, [])  # Placeholder arguments for standalone testing