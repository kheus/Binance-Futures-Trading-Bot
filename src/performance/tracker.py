import logging

from logging.handlers import RotatingFileHandler

import os

import time

import pandas as pd

from src.database.db_handler import get_pending_training_data, update_training_outcome, clean_old_data, execute_query



# Set up logging

os.makedirs("logs", exist_ok=True)

log_file = "logs/tracker.log"

file_handler = RotatingFileHandler(log_file, maxBytes=1_000_000, backupCount=3, encoding="utf-8-sig")

file_handler.setLevel(logging.INFO)

file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

console_handler = logging.StreamHandler()

console_handler.setLevel(logging.INFO)

console_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))



logging.basicConfig(

    level=logging.INFO,

    handlers=[file_handler, console_handler],

    encoding='utf-8'

)



logger = logging.getLogger(__name__)



def calculate_market_direction(symbol, signal_ts):

    """

    Calculate market direction and percentage change after 5 minutes.

    """

    try:

        future_ts = signal_ts + 5 * 60 * 1000  # 5 minutes aprÃ¨s

        window_end_ts = signal_ts + 10 * 60 * 1000  # FenÃªtre de 10 minutes

        query = """

        SELECT close, timestamp FROM price_data 

        WHERE symbol = ? AND timestamp >= ? AND timestamp <= ?

        ORDER BY timestamp ASC LIMIT 1

        """

        # Executer la requÃªte pour obtenir le prix futur

        result = execute_query(query, (symbol, future_ts, window_end_ts))

        if result:

            future_price, future_ts_found = float(result[0][0]), result[0][1]

            query = """

            SELECT price FROM signals 

            WHERE symbol = ? AND timestamp = ?

            """

            signal_result = execute_query(query, (symbol, signal_ts))

            if signal_result:

                signal_price = float(signal_result[0])

                pct_change = ((future_price - signal_price) / signal_price) * 100

                direction = 1 if future_price > signal_price else 0

                logger.info(f"[Market Direction] Found price for {symbol} at {future_ts_found}: {future_price}")

                return direction, pct_change

        # Log des donnees recentes pour debogage

        query = "SELECT timestamp, close FROM price_data WHERE symbol = ? ORDER BY timestamp DESC LIMIT 5"

        recent_prices = execute_query(query, (symbol,))

        logger.debug(f"[Market Direction] Recent prices for {symbol}: {recent_prices}")

        logger.warning(f"[Market Direction] No future price data for {symbol} at {future_ts}")

        return None, None

    except Exception as e:

        logger.error(f"[Market Direction] Error for {symbol} at {signal_ts}: {str(e)}")

        return None, None



def should_retrain_model():

    """

    Determine if the model should be retrained.

    """

    return False  # a implementer selon les critÃ¨res de retraining



def performance_tracker_loop():

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

            # Nettoyage periodique des donnees obsolÃ¨tes

            current_time = time.time()

            if current_time - last_cleanup > cleanup_interval:

                logger.info("[Tracker] Starting database cleanup")

                clean_old_data(retention_days=30, trades_retention_days=90)

                last_cleanup = current_time



            # Recuperer les enregistrements en attente

            pending_records = get_pending_training_data()

            if not pending_records:

                logger.info("[Tracker] No pending records to process")

                time.sleep(300)  # Attendre 5 minutes si aucune donnee

                continue



            total_correct = 0

            processed_records = 0

            for record in pending_records:

                try:

                    if isinstance(record, tuple):

                        record = dict(zip(columns, record))  # âœ… transformation sÃ»re

                    symbol = record['symbol']

                    signal_ts = record['timestamp']

                    action = record['action']

                    record_id = record['id']



                    logger.debug(f"[Tracker] Processing record: ID={record_id}, Symbol={symbol}, TS={signal_ts}")



                    # Calculer la direction du marche

                    direction, pct_change = calculate_market_direction(symbol, signal_ts)

                    if direction is not None:

                        prediction_correct = 1 if direction == (1 if action == 'buy' else 0) else 0

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

                except Exception as e:

                    logger.error(f"[Tracker] Error processing record {record}: {str(e)}")

                    continue  # Passer a l'enregistrement suivant



            # Calculcluer et loggercluer l'accuracy du batch

            if processed_records > 0:

                accuracy = total_correct / processed_records

                logger.info(f"[Tracker] Batch accuracy: {accuracy:.2%} ({total_correct}/{processed_records})")

            else:

                logger.info("[Tracker] No records processed in this batch")



            # Verifier si un retraining est necessaire

            if should_retrain_model():

                logger.info("[Tracker] Starting model retraining...")

                # a implementer : logique pour declencher le retraining



            time.sleep(60)  # Attendre 1 minute avant la prochaine iteration



        except Exception as e:

            logger.error(f"[Tracker] Error in tracker loop: {str(e)}")

            time.sleep(60)  # Attendre avant de reessayer en cas d'erreur



if __name__ == "__main__":

    logger.info("Starting [Tracker] performance tracker")

    performance_tracker_loop()
