# New module: src/performance/tracker.py
import time
import logging
from datetime import datetime, timedelta
from src.database.db_handler import update_training_outcome, get_pending_training_data
from ..processing_core.signal_generator import calculate_market_direction  # Use relative import if in the same package

def should_retrain_model():
    last_retrain = getattr(should_retrain_model, 'last_retrain', 0)
    if time.time() - last_retrain > 7 * 24 * 3600:  # 7 days
        should_retrain_model.last_retrain = time.time()
        return True
    return False

logger = logging.getLogger(__name__)

def performance_tracker_loop():
    """Background process to update trade outcomes"""
    while True:
        try:
            pending_records = get_pending_training_data()
            for record in pending_records:
                symbol = record['symbol']
                signal_ts = record['timestamp']
                try:
                    direction, pct_change = calculate_market_direction(symbol, signal_ts)
                    if direction is not None:
                        update_training_outcome(
                            record_id=record['id'],
                            market_direction=direction,
                            price_change_pct=pct_change,
                            prediction_correct=1 if direction == (1 if record['action'] == 'buy' else 0) else 0
                        )
                        logger.info(f"[Performance Tracking] Updated {symbol} signal: Direction={direction} Change={pct_change:.2f}%")
                    else:
                        logger.warning(f"[Performance Tracking] Insufficient data for {symbol} at {signal_ts}")
                except Exception as e:
                    logger.error(f"[Performance Tracking] Failed to process record {record['id']} - Error: {e}")
            total_correct = sum(
                1 for record in pending_records
                if calculate_market_direction(record['symbol'], record['timestamp'])[0] == (1 if record['action'] == 'buy' else 0)
            )
            accuracy = total_correct / len(pending_records) if pending_records else 0
            logger.info(f"[Performance Tracking] Batch accuracy: {accuracy:.2%}")
            if should_retrain_model():
                logger.info("[Retraining] Starting weekly model retraining...")
                # ... (call model retraining function)
        except Exception as e:
            logger.error(f"[Performance Tracking] Error in tracker loop: {e}")
        if not pending_records:
            time.sleep(300)
            continue
        time.sleep(60)  # Check every minute