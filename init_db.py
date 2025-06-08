import sqlite3

try:
    conn = sqlite3.connect("trading_bot.db")
    with conn:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id TEXT,
                symbol TEXT,
                side TEXT,
                price REAL,
                quantity REAL,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='trades';")
        result = cur.fetchone()
        if result:
            print("Table 'trades' created successfully.")
        else:
            print("Table 'trades' not found after creation attempt.")
    conn.close()
except Exception as e:
    print(f"Error creating table: {e}")