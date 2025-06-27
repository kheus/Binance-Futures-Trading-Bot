try:
    from binance.enums import SIDE_BUY, SIDE_SELL, ORDER_TYPE_MARKET
    from src.trade_execution.correlation_monitor import CorrelationMonitor
    from src.trade_execution.trade_analyzer import TradeAnalyzer
except ImportError:
    SIDE_BUY = 'BUY'
    SIDE_SELL = 'SELL'
    ORDER_TYPE_MARKET = 'MARKET'

import time
import logging
import yaml
from pathlib import Path

logger = logging.getLogger(__name__)

# Chargement de la configuration globale
CONFIG_PATH = Path(__file__).parent.parent.parent / "config" / "config.yaml"
with open(CONFIG_PATH, "r", encoding="utf-8-sig") as f:
    config = yaml.safe_load(f)

# Récupération des clés API et des symboles
BINANCE_API_KEY = config["binance"]["api_key"]
BINANCE_API_SECRET = config["binance"]["api_secret"]
SYMBOLS = config["binance"]["symbols"]

# Importation des classes de trailing stop et de protection contre les crashs
from src.trade_execution.market_crash_protector import MarketCrashProtector
# Initialisation globale du trailing stop manager
ts_manager = None
crash_protector = MarketCrashProtector()
from src.trade_execution.ultra_aggressive_trailing import UltraAgressiveTrailingStop, get_average_fill_price
def init_trailing_stop_manager(client):
    global ts_manager
    ts_manager = UltraAgressiveTrailingStop(client)
    return ts_manager

def get_tick_info(client, symbol):
    exchange_info = client.exchange_info()
    symbol_info = next((s for s in exchange_info["symbols"] if s["symbol"] == symbol), None)
    if not symbol_info:
        raise ValueError(f"Symbol {symbol} not found in exchange info.")

    price_filter = next(f for f in symbol_info['filters'] if f['filterType'] == 'PRICE_FILTER')
    return float(price_filter['tickSize']), int(symbol_info['quantityPrecision'])

def round_to_tick(value, tick_size):
    return round(round(value / tick_size) * tick_size, 8)

def place_order(signal, price, atr, client, symbol, capital, leverage):
    global ts_manager

    if signal not in ["buy", "sell"]:
        logger.error(f"[Order Error] Invalid signal: {signal}")
        return None

    if not all(isinstance(x, (int, float)) for x in [capital, price, leverage]):
        logger.error(f"[Order Error] Invalid parameters")
        return None

    try:
        # Initialisation du manager si nécessaire
        if ts_manager is None:
            ts_manager = init_trailing_stop_manager(client)

        tick_size, quantity_precision = get_tick_info(client, symbol)
        base_qty = capital / price
        qty = round(base_qty * leverage, quantity_precision)

        # Place l'ordre principal
        side = SIDE_BUY if signal == "buy" else SIDE_SELL
        order = client.new_order(
            symbol=symbol,
            side=side,
            type=ORDER_TYPE_MARKET,
            quantity=qty
        )

        # Récupère le prix moyen de remplissage
        avg_price = get_average_fill_price(client, symbol, order['orderId']) or price

        # Initialise le trailing stop ultra-agressif
        position_type = "long" if signal == "buy" else "short"
        ts_manager.initialize_trailing_stop(
            symbol=symbol,
            entry_price=avg_price,
            position_type=position_type,
            quantity=qty,
            atr=atr
        )

        order_details = {
            "order_id": str(order["orderId"]),
            "symbol": symbol,
            "side": signal,
            "quantity": qty,
            "price": avg_price,
            "stop_loss": "TRAILING",  # Indique qu'on utilise trailing stop
            "take_profit": None,      # Optionnel
            "timestamp": int(time.time() * 1000),
            "pnl": 0.0
        }

        return order_details

    except Exception as e:
        logger.error(f"[Order Failed] {e}")
        return None

def update_trailing_stop(client, symbol, signal, current_price, atr, base_qty, existing_sl_order_id):
    """Version modifiée pour utiliser le trailing stop agressif"""
    global ts_manager

    if ts_manager is None:
        ts_manager = init_trailing_stop_manager(client)

    if existing_sl_order_id is None:
        # Initialisation
        position_type = "long" if signal == "buy" else "short"
        return ts_manager.initialize_trailing_stop(
            symbol=symbol,
            entry_price=current_price,
            position_type=position_type,
            quantity=base_qty,
            atr=atr
        )
    else:
        # Mise à jour
        return ts_manager.update_trailing_stop(symbol, current_price)

def place_scaled_take_profits(client, symbol, entry_price, quantity, position_type, atr, ts_manager):
    tp_levels = [
        {"factor": 1.5, "percent": 0.4, "reduce_ts": True},
        {"factor": 3.0, "percent": 0.4, "reduce_ts": True},
        {"factor": 5.0, "percent": 0.2, "close_position": True}
    ]
    for level in tp_levels:
        tp_price = entry_price + (level["factor"] * atr) if position_type == "long" else entry_price - (level["factor"] * atr)
        partial_qty = quantity * level["percent"]
        client.new_order(
            symbol=symbol,
            side=SIDE_SELL if position_type == "long" else SIDE_BUY,
            type="TAKE_PROFIT_MARKET",
            stopPrice=str(tp_price),
            quantity=partial_qty,
            closePosition=level.get("close_position", False),
            priceProtect=True
        )
        if level.get("reduce_ts"):
            ts_manager.adjust_quantity(symbol, quantity * (1 - level["percent"]))

# Dans ta boucle de trading ou lors de chaque tick :
# Assurez-vous que 'symbol' et 'current_price' sont définis avant d'appeler cette section.
# Par exemple :
# symbol = "BTCUSDT"
# current_price = get_current_price_somehow(symbol)
# Example: define symbol before using it
symbol = "BTCUSDT"  # Replace with your desired symbol or logic to set it

# Define or import your Binance client before using it
try:
    from binance.client import Client
    # You should replace 'your_api_key' and 'your_api_secret' with your actual credentials
    client = Client(api_key=BINANCE_API_KEY, api_secret=BINANCE_API_SECRET)
except ImportError:
    client = None
    logger.error("Binance client could not be imported.")

# Define current_price before using it
current_price = None
try:
    # Attempt to get the current price from the client if available
    if client is not None:
        ticker = client.get_symbol_ticker(symbol=symbol)
        current_price = float(ticker['price'])
    # Fallback: define your own method to get the current price if needed
    # current_price = get_current_price_somehow(symbol)
except Exception as e:
    logger.error(f"Failed to get current price for {symbol}: {e}")
    current_price = None

for symbol in SYMBOLS:
    try:
        ticker = client.get_symbol_ticker(symbol=symbol)
        current_price = float(ticker['price'])
        # ... ta logique de trading ici ...
        if current_price is not None and crash_protector.check_market_crash(symbol, current_price):
            ts_manager.close_position(symbol)
            def send_telegram_alert(message):
                logger.warning(f"TELEGRAM ALERT: {message}")
            send_telegram_alert(f"⚠️ CRASH DETECTED - Position closed for {symbol}")
    except Exception as e:
        logger.error(f"Failed to process {symbol}: {e}")

class EnhancedOrderManager:
    def __init__(self, client, symbols):
        self.client = client
        self.ts_manager = UltraAgressiveTrailingStop(client)
        self.crash_protector = MarketCrashProtector()
        self.correlation_monitor = CorrelationMonitor(symbols)
        self.trade_analyzer = TradeAnalyzer()

    def dynamic_position_sizing(self, symbol, capital):
        # Simple example: use all capital divided by current price
        price = self.get_current_price(symbol)
        if price is None or price == 0:
            return 0
        return capital / price

    def place_enhanced_order(self, signal, symbol, capital, leverage):
        corr_matrix = self.correlation_monitor.get_correlation_matrix()
        if corr_matrix is not None and self.is_overexposed(symbol, corr_matrix):
            return None
        atr = self.get_current_atr(symbol)
        price = self.get_current_price(symbol)
        quantity = self.dynamic_position_sizing(symbol, capital)
        order = place_order(signal, price, atr, self.client, symbol, quantity, leverage)
        if order:
            self.ts_manager.initialize_trailing_stop(...)
            self.crash_protector.check_market_crash(symbol, price)
            place_scaled_take_profits(self.client, symbol, price, quantity, signal, atr, self.ts_manager)
            self.trade_analyzer.add_trade(order)
        return order

class UltraAgressiveTrailingStop:
    def __init__(self, client):
        self.client = client
    def initialize_trailing_stop(self, **kwargs):
        pass
    def update_trailing_stop(self, symbol, current_price):
        pass

def get_average_fill_price(client, symbol, order_id):
    return None

