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
        future_ts = signal_ts + 5 * 60 * 1000  # 5 minutes après
        window_end_ts = signal_ts + 10 * 60 * 1000  # Fenêtre de 10 minutes
        query = """
        SELECT close, timestamp FROM price_data 
        WHERE symbol = %s AND timestamp >= %s AND timestamp <= %s
        ORDER BY timestamp ASC LIMIT 1
        """
        # Exécuter la requête pour obtenir le prix futur
        result = execute_query(query, (symbol, future_ts, window_end_ts), fetch=True)
        if result:
            future_price, future_ts_found = float(result[0]['close']), result[0]['timestamp']
            query = """
            SELECT price FROM signals 
            WHERE symbol = %s AND timestamp = %s
            """
            signal_result = execute_query(query, (symbol, signal_ts), fetch=True)
            if signal_result:
                signal_price = float(signal_result[0]['price'])
                pct_change = ((future_price - signal_price) / signal_price) * 100
                direction = 1 if future_price > signal_price else 0
                logger.info(f"[Market Direction] Found price for {symbol} at {future_ts_found}: {future_price}")
                return direction, pct_change

        # Log des données récentes pour débogage
        query = "SELECT timestamp, close FROM price_data WHERE symbol = %s ORDER BY timestamp DESC LIMIT 5"
        recent_prices = execute_query(query, (symbol,), fetch=True)
        logger.debug(f"[Market Direction] Recent prices for {symbol}: {recent_prices}")
        logger.warning(f"[Market Direction] No future price data for {symbol} at {future_ts}")
        return None, None
    except psycopg2.pool.PoolError as e:
        logger.error(f"[Market Direction] Connection pool exhausted for {symbol} at {signal_ts}: {str(e)}")
        return None, None
    except Exception as e:
        logger.error(f"[Market Direction] Error for {symbol} at {signal_ts}: {str(e)}")
        return None, None

def should_retrain_model():
    """
    Determine if the model should be retrained.
    """
    return False  # À implémenter selon les critères de retraining

def performance_tracker_loop(client, symbols):
    """
    Periodically track performance of signals and clean old data.
    """
    last_cleanup = 0
    cleanup_interval = 24 * 3600  # 24 heures

    # Colonnes attendues pour les enregistrements de get_pending_training_data
    columns = [
        'id', 'symbol', 'timestamp', 'prediction', 'action', 'price',
        'indicators', 'market_context', 'market_direction',
        'price_change_pct', 'prediction_correct', 'created_at', 'updated_at'
    ]

    while True:
        try:
            # Nettoyage périodique des données obsolètes
            current_time = time.time()
            if current_time - last_cleanup > cleanup_interval:
                logger.info("[Tracker] Starting database cleanup")
                try:
                    clean_old_data(retention_days=30, trades_retention_days=90)
                    logger.info("[Tracker] Database cleanup completed")
                except psycopg2.pool.PoolError as e:
                    logger.error(f"[Tracker] Connection pool exhausted during cleanup: {str(e)}")
                last_cleanup = current_time

            # Récupérer les enregistrements en attente
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
                    action = record['action']
                    record_id = record['id']

                    logger.debug(f"[Tracker] Processing record: ID={record_id}, Symbol={symbol}, TS={signal_ts}")

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
                        logger.error(f"[Tracker] Insufficient data for {symbol} at {signal_ts}")

                    # Pause pour réduire la pression sur le pool de connexions
                    eventlet.sleep(0.1)
                except Exception as e:
                    logger.error(f"[Tracker] Error processing record {record}: {str(e)}")
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
                # À implémenter : logic for retraining the model

            time.sleep(60)  # Wait before next iteration

        except psycopg2.pool.PoolError as e:
            logger.error(f"[Tracker] Connection pool exhausted in tracker loop: {str(e)}")
            time.sleep(60)  # Wait before retrying
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