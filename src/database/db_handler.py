import psycopg2
from psycopg2 import sql
import logging
import os

logger = logging.getLogger(__name__)

import sys
import locale
if sys.stdout.encoding != 'UTF-8':
    sys.stdout.reconfigure(encoding='utf-8')
if sys.stderr.encoding != 'UTF-8':
    sys.stderr.reconfigure(encoding='utf-8')

DB_NAME = "trading_bot_db"
DB_USER = "postgres"
DB_PASSWORD = "Datas12"
DB_HOST = "localhost"
DB_PORT = "5432"

def get_db_connection():
    try:
        conn_params = {"dbname": DB_NAME, "user": DB_USER, "password": DB_PASSWORD, "host": DB_HOST, "port": DB_PORT, "client_encoding": "UTF8"}
        logger.info(f"Attempting connection with params: {conn_params}")
        print(f"Attempting connection with params: {conn_params}")
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        conn.set_client_encoding('UTF8')

        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS trades (
                    id SERIAL PRIMARY KEY,
                    order_id VARCHAR(50),
                    symbol VARCHAR(20),
                    side VARCHAR(10),
                    price NUMERIC,
                    quantity NUMERIC,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)

            conn.commit()
        logger.info("Connected to database and ensured trades table exists.")
        print("Connected to database and ensured trades table exists.")
        return conn
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Database connection error: {error_msg}")
        print(f"Database connection error: {error_msg}")
        return None

def sanitize(value):
    """Basic sanitize function to strip whitespace."""
    if isinstance(value, str):
        return value.strip()
    return value

def insert_trade(order_details):
    conn = get_db_connection()
    if conn is None:
        return
    try:
        with conn.cursor() as cur:
            print("Order details raw dump:", repr(order_details))
            print("Order details types:", {k: type(v) for k, v in order_details.items()})
            logger.info(f"Inserting trade with order details: {order_details}")
            cur.execute(
                """
                INSERT INTO trades (order_id, symbol, side, price, quantity)
                VALUES (%s, %s, %s, %s, %s)
                """,
               (
                    sanitize(str(order_details["order_id"])),
                    sanitize(order_details["symbol"]),
                    sanitize(order_details["side"]),
                    float(order_details["price"]),
                    float(order_details["quantity"])
               )
            )

        conn.commit()
        logger.info(f"Trade inserted: {order_details}")
        print(f"Trade inserted: {order_details}")
    except Exception as e:
        error_msg = str(e).encode('utf-8', errors='replace').decode('utf-8')
        logger.error(f"Error inserting trade: {error_msg}")
        print(f"Error inserting trade: {error_msg}")
    finally:
        conn.close()