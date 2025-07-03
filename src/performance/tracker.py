import logging
import time
import pandas as pd
import numpy as np
from src.database.db_handler import get_pending_training_data, update_training_outcome, clean_old_data, execute_query
from tabulate import tabulate
import psycopg2.pool
import eventlet

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
    """
    Periodically track performance of signals and clean old data.
    """
    last_cleanup = 0
    cleanup_interval = 24 * 3600  # 24 hours

    # Expected columns for records from get_pending_training_data
    columns = [
        'id', 'symbol', 'timestamp', 'indicators', 'market_context',
        'market_direction', 'price_change_pct', 'prediction_correct',
        'created_at', 'updated_at'
    ]

    while True:
        try:
            # Periodic cleanup of old data
            current_time = time.time()
            if current_time - last_cleanup > cleanup_interval:
                logger.info("[Tracker] Starting database cleanup")
                try:
                    clean_old_data(retention_days=30, trades_retention_days=90)
                    logger.info("[Tracker] Database cleanup completed")
                except psycopg2.pool.PoolError as e:
                    logger.error(f"[Tracker] Connection pool exhausted during cleanup: {str(e)}")
                last_cleanup = current_time

            # Retrieve pending records
            pending_records = get_pending_training_data()
            if not pending_records:
                logger.info("[Tracker] No pending records to process")
                time.sleep(300)  # Wait 5 minutes if no data
                continue

            total_correct = 0
            processed_records = 0
            for record in pending_records:
                try:
                    if isinstance(record, tuple):
                        record = dict(zip(columns, record))  # Safe transformation
                    symbol = record['symbol']
                    if symbol not in symbols:  # Filter by the list of symbols
                        logger.debug(f"[Tracker] Skipping record for {symbol} (not in SYMBOLS)")
                        continue
                    signal_ts = record['timestamp']
                    record_id = record['id']
                    market_context = record.get('market_context', {})

                    # Infer action from signals table or market_context
                    action = get_action_from_signals(symbol, signal_ts)
                    if action is None:
                        if market_context.get('trend_up') and market_context.get('macd_bullish'):
                            action = 'buy'
                        elif market_context.get('trend_down'):
                            action = 'sell'
                        else:
                            action = 'hold'
                    logger.debug(f"[Tracker] Inferred action for {symbol} (ID={record_id}): {action}")

                    logger.debug(f"[Tracker] Processing record: ID={record_id}, Symbol={symbol}, TS={signal_ts}, Action={action}")

                    # Calculate market direction
                    direction, pct_change = calculate_market_direction(symbol, signal_ts)
                    if direction is not None:
                        prediction_correct = 1 if direction == (1 if action == 'buy' else 0) else 0
                        if isinstance(prediction_correct, np.bool_):
                            prediction_correct = bool(prediction_correct)
                        update_training_outcome(
                            record_id=record_id,
                            market_direction=direction,
                            price_change_pct=pct_change,
                            prediction_correct=prediction_correct
                        )
                        logger.info(
                            f"[Tracker] Updated {symbol} signal: ID={record_id}, "
                            f"Direction={pct_change:.2f}%, Correct={prediction_correct}"
                        )
                        processed_records += 1
                        total_correct += prediction_correct
                    else:
                        logger.warning(f"[Tracker] Insufficient data for {symbol} at {signal_ts}")

                    # Pause to reduce pressure on connection pool
                    eventlet.sleep(0.1)
                except Exception as e:
                    logger.error(f"[Tracker] Error processing record ID={record.get('id', 'unknown')}: {str(e)}")
                    continue

            # Calculate and log accuracy for the batch
            if processed_records > 0:
                accuracy = total_correct / processed_records
                logger.info(f"[Tracker] Batch accuracy: {accuracy:.2%} ({total_correct}/{processed_records})")
                log_performance_as_table(record_id, symbol, accuracy, processed_records)
            else:
                logger.info("[Tracker] No records processed in this batch")

            # Verify if the model should be retrained
            if should_retrain_model():
                logger.info("[Tracker] Starting model retraining...")
                # To be implemented: logic for retraining the model

            time.sleep(60)  # Wait before next iteration

        except psycopg2.pool.PoolError as e:
            logger.error(f"[Tracker] Connection pool exhausted in tracker loop: {str(e)}")
            time.sleep(60)  # Wait before retrying
        except KeyboardInterrupt:
            logger.info("[Tracker] Received interrupt signal, shutting down gracefully...")
            break
        except Exception as e:
            logger.error(f"[Tracker] Error in tracker loop: {str(e)}")
            time.sleep(60)  # Wait before retrying

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