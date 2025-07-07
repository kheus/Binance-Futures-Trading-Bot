import psycopg2
import pandas as pd
from src.database.db_handler import get_db_connection

try:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            query = """
            INSERT INTO price_data (symbol, timestamp, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, timestamp) DO NOTHING
            """
            params = (
                'BTCUSDT',
                pd.Timestamp('2025-06-30 05:54:59.999'),
                108161.0,
                108189.9,
                108142.8,
                108189.8,
                121.277
            )
            cur.execute(query, params)
            conn.commit()
            print("Insertion réussie pour BTCUSDT")
except psycopg2.Error as e:
    print(f"Erreur PostgreSQL: {e.pgerror}, code: {e.pgcode}")
except Exception as e:
    print(f"Erreur inattendue: {str(e)}, type: {type(e).__name__}")