# src/monitoring/init_db.py
import sqlite3
import logging

# Configuration
db_path = 'market_data.db'
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"]

# Logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - [DB Init] %(message)s")
logger = logging.getLogger()

def create_tables():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # Table des prix
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS price_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            timestamp INTEGER NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            volume REAL NOT NULL
        )
        ''')
        
        # Table des indicateurs
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS indicators (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            timestamp INTEGER NOT NULL,
            rsi REAL,
            macd REAL,
            macd_signal REAL,
            macd_hist REAL,
            bollinger_upper REAL,
            bollinger_middle REAL,
            bollinger_lower REAL
        )
        ''')
        
        # Table des signaux
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            timestamp INTEGER NOT NULL,
            signal_type TEXT NOT NULL,
            strength REAL
        )
        ''')
        
        # Table des trades
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            timestamp INTEGER NOT NULL,
            side TEXT NOT NULL,
            price REAL NOT NULL,
            quantity REAL NOT NULL,
            order_id TEXT
        )
        ''')
        
        conn.commit()
        logger.info("Tables created successfully")
        
        # Creation d'exemples de donnees
        from datetime import datetime
        import time
        
        timestamp = int(time.time() * 1000)
        for symbol in SYMBOLS:
            # Prix
            cursor.execute('''
            INSERT INTO price_data (symbol, timestamp, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (symbol, timestamp, 50000, 51000, 49500, 50500, 1000))
            
            # Indicateurs
            cursor.execute('''
            INSERT INTO indicators (symbol, timestamp, rsi, macd, macd_signal, macd_hist, bollinger_upper, bollinger_middle, bollinger_lower)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (symbol, timestamp, 65.2, 120.5, 115.3, 5.2, 52000, 50500, 49000))
            
            # Signaux
            cursor.execute('''
            INSERT INTO signals (symbol, timestamp, signal_type, strength)
            VALUES (?, ?, ?, ?)
            ''', (symbol, timestamp, "BUY", 0.85))
            
            # Trades
            cursor.execute('''
            INSERT INTO trades (symbol, timestamp, side, price, quantity, order_id)
            VALUES (?, ?, ?, ?, ?, ?)
            ''', (symbol, timestamp, "BUY", 50500, 0.1, "ORD-12345"))
        
        conn.commit()
        logger.info("Sample data inserted")
        
    except Exception as e:
        logger.error(f"Error creating tables: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    create_tables()
