import psycopg2
import yaml

def get_db_connection():
    try:
        with open("config/db_config.yaml", "r") as f:
            config = yaml.safe_load(f)
        conn = psycopg2.connect(
            host=config["postgresql"]["host"],
            port=config["postgresql"]["port"],
            database=config["postgresql"]["database"],
            user=config["postgresql"]["user"],
            password=config["postgresql"]["password"]
        )
        return conn
    except Exception as e:
        print(f"Database connection error: {e}")
        return None

def insert_trade(order_details):
    try:
        conn = get_db_connection()
        if conn is None:
            return
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO trades (symbol, side, quantity, price, stop_loss, take_profit, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            order_details["symbol"],
            order_details["side"],
            order_details["quantity"],
            order_details["price"],
            order_details["stop_loss"],
            order_details["take_profit"],
            order_details["timestamp"]
        ))
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Trade insertion error: {e}")
