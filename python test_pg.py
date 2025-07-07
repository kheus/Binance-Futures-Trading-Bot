# test_pg.py
import yaml
import psycopg2

with open("config/db_config.yaml", "r", encoding="utf-8-sig") as f:
    config = yaml.safe_load(f)

pg_conf = config['database']['postgresql']

conn = psycopg2.connect(
    dbname="test",
    user="test",
    password="test",
    host="localhost",
    port="5432"
)


print("âœ… Connexion reussie")
conn.close()

