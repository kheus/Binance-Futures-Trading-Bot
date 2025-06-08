import sqlite3

try:
    conn = sqlite3.connect("trading_bot.db")
    cur = conn.cursor()
    cur.execute("SELECT * FROM trades;")
    rows = cur.fetchall()
    if not rows:
        print("No trades found in the database.")
    else:
        print("Trades in database:")
        for row in rows:
            print(row)
    conn.close()
except Exception as e:
    print(f"Error querying database: {e}")