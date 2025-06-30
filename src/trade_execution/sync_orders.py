
from operator import ge
from src.database.db_handler import insert_trade, insert_order_if_missing
from src.trade_execution.ultra_aggressive_trailing import UltraAgressiveTrailingStop
import logging
import time
import pandas as pd
from binance.um_futures import UMFutures
from sqlalchemy import create_engine
import yaml
import os
import talib

logger = logging.getLogger(__name__)

def load_db_config():
    """Charge la configuration de la base de données depuis db_config.yaml."""
    config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../config/db_config.yaml'))
    with open(config_path, 'r', encoding='utf-8-sig') as f:
        return yaml.safe_load(f)['database']['postgresql']

def sync_binance_trades_with_postgres(client: UMFutures, symbol_list, ts_manager: UltraAgressiveTrailingStop):
    """Synchronise les ordres et trades entre Binance et PostgreSQL, y compris les trades manquants."""
    try:
        logger.info("Starting trade sync process...")
        start_time = time.time()
        
        # Étape 1 : Récupérer tous les ordres (ouverts et exécutés) depuis Binance
        all_orders = []
        for symbol in symbol_list:
            try:
                orders = client.get_all_orders(symbol=symbol, limit=100)
                for order in orders:
                    if order['status'] in ['NEW', 'PARTIALLY_FILLED', 'FILLED']:
                        order_data = {
                            'order_id': str(order['orderId']),
                            'symbol': order['symbol'],
                            'side': order['side'].lower(),
                            'quantity': float(order['origQty']),
                            'price': float(order['avgPrice'] or order['price']),
                            'timestamp': order['time'],
                            'status': order['status'].lower(),
                            'is_trailing': order.get('type') == 'TRAILING_STOP_MARKET'
                        }
                        all_orders.append(order_data)
            except Exception as e:
                logger.error(f"Error fetching orders for {symbol}: {str(e)}")

        # Étape 2 : Récupérer les trades existants dans PostgreSQL avec SQLAlchemy
        db_config = load_db_config()
        engine = create_engine(f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}")
        query = "SELECT order_id, symbol FROM trades"
        existing_trades = pd.read_sql(query, engine).to_dict('records')
        existing_trades_set = {(t['order_id'], t['symbol']) for t in existing_trades}

        # Étape 3 : Identifier les trades manquants (FILLED mais absents de PostgreSQL)
        synced_count = 0
        for order in all_orders:
            try:
                if order['status'] == 'filled' and (order['order_id'], order['symbol']) not in existing_trades_set:
                    trade_data = {
                        'order_id': order['order_id'],
                        'symbol': order['symbol'],
                        'side': order['side'],
                        'quantity': order['quantity'],
                        'price': order['price'],
                        'stop_loss': None,
                        'take_profit': None,
                        'timestamp': order['timestamp'],
                        'pnl': 0.0,
                        'is_trailing': order['is_trailing']
                    }
                    insert_trade(trade_data)
                    logger.info(f"Inserted missing trade for {order['symbol']}: {order['order_id']}")

                    if not order['is_trailing']:
                        atr = get_current_atr(client, order['symbol'])
                        position_type = 'long' if order['side'] == 'buy' else 'short'
                        ts_manager.initialize_trailing_stop(
                            symbol=order['symbol'],
                            entry_price=order['price'],
                            position_type=position_type,
                            quantity=order['quantity'],
                            atr=atr
                        )
                        logger.info(f"Initialized trailing stop for recovered trade {order['order_id']} ({order['symbol']})")
                    synced_count += 1

                if order['status'] in ['new', 'partially_filled']:
                    if insert_order_if_missing(order):
                        synced_count += 1
            except Exception as e:
                logger.error(f"Error processing order {order['order_id']} for {order['symbol']}: {str(e)}")

        # Étape 4 : Mettre à jour les trailing stops pour les trades existants
        for order in all_orders:
            if order['status'] == 'filled' and order['is_trailing']:
                current_price = get_current_price(client, order['symbol'])
                if current_price:
                    ts_manager.update_trailing_stop(order['symbol'], current_price)
                    logger.info(f"Updated trailing stop for {order['symbol']} at price {current_price}")

        duration = time.time() - start_time
        logger.info(f"Trade sync completed in {duration:.2f}s. Synced {synced_count} trades/orders.")
        return synced_count
        
    except Exception as e:
        logger.error(f"Trade sync failed completely: {str(e)}", exc_info=True)
        return 0

def get_current_price(client: UMFutures, symbol: str) -> float:
    try:
        ticker = client.ticker_price(symbol=symbol)
        return float(ticker['price'])
    except Exception as e:
        logger.error(f"Failed to get current price for {symbol}: {str(e)}")
        return None

def get_current_atr(client: UMFutures, symbol: str) -> float:
    try:
        klines = client.klines(symbol=symbol, interval='1h', limit=15)
        df = pd.DataFrame(klines, columns=["open_time", "open", "high", "low", "close", "volume",
                                          "close_time", "quote_asset_vol", "num_trades", "taker_buy_base_vol",
                                          "taker_buy_quote_vol", "ignore"])
        df = df[["high", "low", "close"]].astype(float)
        atr = talib.ATR(df['high'], df['low'], df['close'], timeperiod=14).iloc[-1]
        return float(atr) if not pd.isna(atr) else 0.0
    except Exception as e:
        logger.error(f"Failed to calculate ATR for {symbol}: {str(e)}")
        return 0.0

#**Changements effectués** :
#- Ajouté la fonction `load_db_config` pour charger les identifiants depuis `db_config.yaml`.
#- Utilisé les identifiants du fichier de configuration pour créer la connexion SQLAlchemy, remplaçant les valeurs codées en dur (`your_user`, `your_password`).

### 2. **Corriger l'erreur WebSocket**
#L'erreur `AttributeError: 'UMFuturesWebsocketClient' object has no attribute 'start'` indique que la méthode `start` n'est pas disponible dans `UMFuturesWebsocketClient`. Dans la bibliothèque `binance-connector-python`, la gestion des WebSockets est différente, et il faut utiliser un gestionnaire de connexions approprié. Nous allons modifier `main_bot.py` pour utiliser correctement le client WebSocket.

# **Mettre à jour `main_bot.py`** :