import logging
import numpy as np
import time
from binance.enums import SIDE_BUY, SIDE_SELL, ORDER_TYPE_MARKET

logger = logging.getLogger(__name__)

class UltraAgressiveTrailingStop:
    def __init__(self, client):
        self.client = client
        self.active_stops = {}  # {symbol: {'order_id': str, 'last_price': float, 'best_price': float}}
    
    def get_tick_info(self, symbol):
        """Récupère les informations de tick size et précision"""
        exchange_info = self.client.exchange_info()
        symbol_info = next((s for s in exchange_info["symbols"] if s["symbol"] == symbol), None)
        if not symbol_info:
            raise ValueError(f"Symbol {symbol} not found in exchange info.")
        
        price_filter = next(f for f in symbol_info['filters'] if f['filterType'] == 'PRICE_FILTER')
        lot_size = next(f for f in symbol_info['filters'] if f['filterType'] == 'LOT_SIZE')
        return float(price_filter['tickSize']), int(symbol_info['quantityPrecision']), float(lot_size['minQty'])

    def round_to_tick(self, value, tick_size):
        """Arrondi au tick size près"""
        return round(round(value / tick_size) * tick_size, 8)

    def calculate_aggressive_callback(self, atr, price, position_type, profit_pct):
        """
        Calcule un callback rate ultra-agressif basé sur:
        - La volatilité (ATR)
        - Le type de position
        - Le profit actuel
        """
        # Base callback basé sur ATR (plus agressif pour les petites variations)
        base_callback = min(2.0, max(0.1, (0.5 * atr / price * 100)))
        
        # Ajustement basé sur le profit
        if profit_pct > 0:
            # Réduit le callback quand on est en profit
            profit_adjustment = max(0.3, 1.0 - (profit_pct / 5.0))  # Plus on a de profit, plus on réduit l'agressivité
            base_callback *= profit_adjustment
        
        # Garantit un minimum très serré
        final_callback = max(0.1, base_callback)  # Jamais en dessous de 0.1%
        
        # Encore plus agressif si on approche du seuil de rentabilité
        if -0.5 < profit_pct < 0.5:
            final_callback = max(final_callback, 0.3)  # Au moins 0.3% près du point d'entrée
        
        return round(final_callback, 1)

    def initialize_trailing_stop(self, symbol, entry_price, position_type, quantity, atr):
        """Initialise un trailing stop ultra-agressif"""
        tick_size, quantity_precision, min_qty = self.get_tick_info(symbol)
        current_price = self.get_current_price(symbol)
        
        # Vérifie si la quantité est valide
        if quantity < min_qty:
            logger.error(f"Quantity {quantity} below minimum {min_qty} for {symbol}")
            return None
        
        # Calcule le profit actuel en %
        profit_pct = ((current_price - entry_price) / entry_price * 100) if position_type == "long" \
                    else ((entry_price - current_price) / entry_price * 100)
        
        callback_rate = self.calculate_aggressive_callback(atr, current_price, position_type, profit_pct)
        side = SIDE_SELL if position_type == "long" else SIDE_BUY
        
        try:
            order = self.client.new_order(
                symbol=symbol,
                side=side,
                type="TRAILING_STOP_MARKET",
                quantity=quantity,
                callbackRate=callback_rate,
                timeInForce="GTC",
                workingType="MARK_PRICE"
            )
            
            logger.info(f"[Ultra Aggressive TS] Initialized for {symbol} at {callback_rate}%")
            
            # Stocke les informations du stop
            self.active_stops[symbol] = {
                'order_id': str(order['orderId']),
                'entry_price': entry_price,
                'last_price': current_price,
                'best_price': current_price if position_type == "long" else current_price,
                'quantity': quantity,
                'position_type': position_type,
                'atr': atr
            }
            
            return order['orderId']
        
        except Exception as e:
            logger.error(f"[Ultra Aggressive TS] Failed to initialize for {symbol}: {e}")
            return None

    def update_trailing_stop(self, symbol, current_price):
        """Met à jour le trailing stop de manière ultra-agressive"""
        if symbol not in self.active_stops:
            logger.warning(f"No active trailing stop for {symbol}")
            return None
        
        stop_info = self.active_stops[symbol]
        position_type = stop_info['position_type']
        entry_price = stop_info['entry_price']
        atr = stop_info['atr']
        
        # Met à jour le meilleur prix atteint
        if position_type == "long":
            stop_info['best_price'] = max(stop_info['best_price'], current_price)
        else:  # short
            stop_info['best_price'] = min(stop_info['best_price'], current_price)
        
        # Calcule le profit actuel
        profit_pct = ((current_price - entry_price) / entry_price * 100) if position_type == "long" \
                    else ((entry_price - current_price) / entry_price * 100)
        
        # Détermine si une mise à jour est nécessaire (seuil très serré)
        price_change_pct = abs((current_price - stop_info['last_price']) / stop_info['last_price'] * 100)
        update_threshold = 0.05  # Seuil très agressif de 0.05% de changement
        
        if price_change_pct >= update_threshold:
            try:
                # Annule l'ancien ordre
                self.client.cancel_order(
                    symbol=symbol,
                    orderId=stop_info['order_id']
                )
                
                # Recalcule le callback rate
                new_callback = self.calculate_aggressive_callback(atr, current_price, position_type, profit_pct)
                
                # Place le nouvel ordre
                side = SIDE_SELL if position_type == "long" else SIDE_BUY
                order = self.client.new_order(
                    symbol=symbol,
                    side=side,
                    type="TRAILING_STOP_MARKET",
                    quantity=stop_info['quantity'],
                    callbackRate=new_callback,
                    timeInForce="GTC",
                    workingType="MARK_PRICE"
                )
                
                logger.info(f"[Ultra Aggressive TS] Updated for {symbol} at {new_callback}% (Profit: {profit_pct:.2f}%)")
                
                # Met à jour les informations
                stop_info['order_id'] = str(order['orderId'])
                stop_info['last_price'] = current_price
                
                return order['orderId']
            
            except Exception as e:
                logger.error(f"[Ultra Aggressive TS] Failed to update for {symbol}: {e}")
                # En cas d'échec, essaie de replacer l'ordre avec les anciens paramètres
                try:
                    order = self.client.new_order(
                        symbol=symbol,
                        side=SIDE_SELL if position_type == "long" else SIDE_BUY,
                        type="TRAILING_STOP_MARKET",
                        quantity=stop_info['quantity'],
                        callbackRate=0.1,  # Callback minimum ultra serré
                        timeInForce="GTC",
                        workingType="MARK_PRICE"
                    )
                    stop_info['order_id'] = str(order['orderId'])
                    logger.warning(f"[Ultra Aggressive TS] Replaced with minimum callback for {symbol}")
                    return order['orderId']
                except Exception as e2:
                    logger.error(f"[Ultra Aggressive TS] Critical fail for {symbol}: {e2}")
                    return None
        
        return stop_info['order_id']

    def get_current_price(self, symbol):
        """Récupère le prix actuel du marché"""
        ticker = self.client.get_symbol_ticker(symbol=symbol)
        return float(ticker['price'])

    def monitor_and_update(self, symbol):
        """Surveille et met à jour le trailing stop en continu"""
        if symbol not in self.active_stops:
            return
        
        current_price = self.get_current_price(symbol)
        self.update_trailing_stop(symbol, current_price)

    def close_position(self, symbol):
        """Ferme la position et supprime le trailing stop"""
        if symbol not in self.active_stops:
            return
        
        try:
            # Annule l'ordre de trailing stop
            self.client.cancel_order(
                symbol=symbol,
                orderId=self.active_stops[symbol]['order_id']
            )
            logger.info(f"[Ultra Aggressive TS] Closed for {symbol}")
        except Exception as e:
            logger.error(f"[Ultra Aggressive TS] Error closing for {symbol}: {e}")
        
        # Supprime des stops actifs
        if symbol in self.active_stops:
            del self.active_stops[symbol]

def place_order_with_ultra_aggressive_stop(client, signal, price, atr, symbol, capital, leverage):
    """
    Version modifiée de place_order avec trailing stop ultra-agressif
    """
    if signal not in ["buy", "sell"]:
        logger.error(f"[Order Error] Invalid signal: {signal}")
        return None

    # Initialisation du trailing stop manager
    ts_manager = UltraAgressiveTrailingStop(client)
    
    try:
        # Récupère les infos de tick
        tick_size, quantity_precision, min_qty = ts_manager.get_tick_info(symbol)
        
        # Calcule la quantité
        base_qty = capital / price
        qty = round(base_qty * leverage, quantity_precision)
        
        if qty < min_qty:
            logger.error(f"Calculated quantity {qty} below minimum {min_qty} for {symbol}")
            return None

        # Change le levier
        client.change_leverage(symbol=symbol, leverage=leverage)
        
        # Place l'ordre principal
        side = SIDE_BUY if signal == "buy" else SIDE_SELL
        order = client.new_order(
            symbol=symbol,
            side=side,
            type=ORDER_TYPE_MARKET,
            quantity=qty
        )
        
        logger.info(f"[ORDER] {signal.upper()} {qty} {symbol} @ {price}")
        time.sleep(0.5)  # Pause pour laisser l'ordre se remplir
        
        # Récupère le prix de remplissage moyen
        avg_fill_price = get_average_fill_price(client, symbol, order['orderId'])
        
        # Initialise le trailing stop ultra-agressif
        position_type = "long" if signal == "buy" else "short"
        ts_manager.initialize_trailing_stop(
            symbol=symbol,
            entry_price=avg_fill_price,
            position_type=position_type,
            quantity=qty,
            atr=atr
        )
        
        # Crée l'objet order_details
        order_details = {
            "order_id": str(order["orderId"]),
            "symbol": symbol,
            "side": signal,
            "quantity": qty,
            "price": avg_fill_price,
            "stop_loss": None,  # Le trailing stop gère ça
            "take_profit": None,  # Optionnel: pourrait être ajouté
            "timestamp": int(time.time() * 1000),
            "pnl": 0.0,
            "trailing_stop": True
        }
        
        return order_details
    
    except Exception as e:
        logger.error(f"[Order Failed] {e}")
        return None

def get_average_fill_price(client, symbol, order_id):
    """Récupère le prix de remplissage moyen d'un ordre"""
    try:
        order = client.get_order(symbol=symbol, orderId=order_id)
        if 'fills' in order and len(order['fills']) > 0:
            total_qty = 0.0
            total_value = 0.0
            for fill in order['fills']:
                fill_qty = float(fill['qty'])
                fill_price = float(fill['price'])
                total_qty += fill_qty
                total_value += fill_qty * fill_price
            return total_value / total_qty if total_qty > 0 else float(order['price'])
        return float(order['price'])
    except Exception as e:
        logger.error(f"Error getting fill price: {e}")
        return None

def monitor_active_positions(client, ts_manager, symbols_to_monitor, interval=5):
    """
    Surveille en continu les positions et met à jour les trailing stops
    """
    while True:
        try:
            for symbol in symbols_to_monitor:
                ts_manager.monitor_and_update(symbol)
            time.sleep(interval)
        except Exception as e:
            logger.error(f"Error in monitoring loop: {e}")
            time.sleep(10)  # Pause plus longue en cas d'erreur