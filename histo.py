import ccxt
import pandas as pd
import time
import os

# Initialiser l'exchange
exchange = ccxt.binance({
    'enableRateLimit': True,
    'apiKey': 'tjmxSoMLy22RZEWO7sTbmMPa6t2wEHrYZElLAKep8VlAF1SbGlpVm9OIWGTIuJl6',  # Facultatif
    'secret': 'Z3hF9Tf83pP2E0KCx31n61hWxEdCTsMgNmVj35qzX68uaSyvXTo2wqmWBA8StR3N'  # Facultatif
})

# Paramètres
symbols = ['BTC/USDT', 'ETH/USDT', 'XRP/USDT']  # Liste des symboles à collecter
timeframe = '5m'  # Timeframe de 5 minutes
since = exchange.parse8601('2025-08-02T11:14:00:00Z')  # Date de début august, 02, 2025
limit = 1000  # Nombre de bougies par requête
output_dir = './backtest'  # Dossier pour sauvegarder les fichiers

# Créer le dossier de sortie si nécessaire
os.makedirs(output_dir, exist_ok=True)

# Télécharger les données pour chaque symbole
for symbol in symbols:
    all_ohlcv = []
    current_since = since
    symbol_filename = symbol.replace('/', '')  # Ex. : BTCUSDT
    output_path = os.path.join(output_dir, f'{symbol_filename}.csv')
    
    print(f"Téléchargement des données pour {symbol}...")
    while current_since < exchange.milliseconds():
        try:
            ohlcv = exchange.fetch_ohlcv(symbol, timeframe, current_since, limit)
            if not ohlcv:
                break
            all_ohlcv.extend(ohlcv)
            current_since = ohlcv[-1][0] + 1  # Passer à la bougie suivante
            time.sleep(exchange.rateLimit / 1000)  # Respecter la limite de requêtes
        except Exception as e:
            print(f"Erreur lors du téléchargement pour {symbol}: {e}")
            break
    
    # Créer un DataFrame
    if all_ohlcv:
        df = pd.DataFrame(all_ohlcv, columns=['datetime', 'open', 'high', 'low', 'close', 'volume'])
        df['datetime'] = pd.to_datetime(df['datetime'], unit='ms')
        df = df[['datetime', 'open', 'high', 'low', 'close', 'volume']].ffill().bfill()

        df.to_csv(output_path, index=False)
        print(f"Fichier {output_path} créé avec succès ({len(df)} lignes).")