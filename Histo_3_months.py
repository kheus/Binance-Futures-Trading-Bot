import pandas as pd
import datetime
import os

# ===================== CONFIG =====================
csv_files = [
    r"C:\Users\Cheikh\binance-trading-bot\backtest\BTCUSDT.csv",
    r"C:\Users\Cheikh\binance-trading-bot\backtest\ETHUSDT.csv",
    r"C:\Users\Cheikh\binance-trading-bot\backtest\XRPUSDT.csv"
]
date_column = "datetime"  # Nom de la colonne date dans vos CSV
# ==================================================

def filter_last_3_months(csv_file, date_column="datetime"):
    if not os.path.exists(csv_file):
        print(f"Le fichier {csv_file} n'existe pas.")
        return

    # Lire le CSV
    df = pd.read_csv(csv_file)
    if date_column not in df.columns:
        print(f"La colonne {date_column} n'existe pas dans {csv_file}.")
        return

    # Conversion en datetime
    df[date_column] = pd.to_datetime(df[date_column], errors='coerce')
    df = df.dropna(subset=[date_column])
    df = df.sort_values(date_column)

    # Filtrer les 3 derniers mois
    three_months_ago = datetime.datetime.now() - pd.DateOffset(months=3)
    df_recent = df[df[date_column] >= three_months_ago] # 3 months ago

    if df_recent.empty:
        print(f"Aucune donnée récente dans {csv_file}.")
        return

    # Nouveau nom de fichier
    base, ext = os.path.splitext(csv_file)
    output_csv = f"{base}_month{ext}"

    # Sauvegarder le CSV réduit
    df_recent.to_csv(output_csv, index=False)
    print(f"{output_csv} créé ({len(df_recent)} lignes conservées)")

# ===================== EXECUTION =====================
for file in csv_files:
    filter_last_3_months(file, date_column)
