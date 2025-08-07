import backtrader as bt
import backtrader.indicators as btind
import pandas as pd
import numpy as np
import datetime
import logging
import json
from scipy.stats import norm

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

class MyTradingStrategy(bt.Strategy):
    params = (
        ('initial_stop_loss_pct', 0.02),  # Stop-loss initial de 2%
        ('take_profit_multiples', [1.5, 3.0, 5.0]),  # Multiples ATR pour TPs
        ('trailing_atr_multiplier', 0.8),  # Multiplicateur pour trailing stop
        ('trailing_min_pct', 0.002),  # Trailing stop minimum de 0.2%
        ('trailing_max_pct', 0.03),  # Trailing stop maximum de 3%
        ('adx_trend_threshold', 30),  # Seuil ADX pour tendance
        ('adx_range_threshold', 15),  # Seuil ADX pour range
        ('rsi_range_low', 40),  # Seuil bas RSI pour range
        ('rsi_range_high', 60),  # Seuil haut RSI pour range
        ('atr_range_threshold', 30),  # Seuil ATR pour range
        ('position_size_ratio', 0.95),  # Ratio du capital par position
        ('leverage', 10),  # Effet de levier
        ('max_concurrent_trades', 3),  # Maximum de trades simultanés
        ('var_confidence_level', 0.95),  # Niveau de confiance pour VaR (95%)
        ('kelly_fraction', 0.5),  # Fraction du Kelly Criterion pour sizing
        ('min_volume_threshold', 1000),  # Volume minimum pour valider un trade
    )

    def __init__(self):
        self.active_positions = {}  # Dictionnaire pour suivre les positions par actif
        self.orders = {}  # Dictionnaire pour suivre les ordres par actif
        self.stop_loss_orders = {}  # Dictionnaire pour stop-loss par actif
        self.take_profit_orders = {}  # Dictionnaire pour take-profits par actif
        self.entry_prices = {}  # Prix d'entrée par actif
        self.open_trades_count = 0  # Compteur de trades ouverts

        # Initialisation des indicateurs par actif
        for data in self.datas:
            symbol = data._name
            self.active_positions[symbol] = None
            self.orders[symbol] = None
            self.stop_loss_orders[symbol] = None
            self.take_profit_orders[symbol] = []
            self.entry_prices[symbol] = 0.0

            # Indicateurs
            setattr(self, f'rsi_{symbol}', btind.RSI(data, period=14))
            setattr(self, f'macd_{symbol}', btind.MACD(data))
            setattr(self, f'macd_signal_{symbol}', getattr(self, f'macd_{symbol}').signal)
            setattr(self, f'adx_{symbol}', btind.ADX(data, period=14))
            setattr(self, f'ema20_{symbol}', btind.EMA(data, period=20))
            setattr(self, f'ema50_{symbol}', btind.EMA(data, period=50))
            setattr(self, f'atr_{symbol}', btind.ATR(data, period=14))
            setattr(self, f'roc_{symbol}', btind.ROC(data, period=5))

        self.last_signal = {}  # Dernier signal par actif
        logger.info("Stratégie initialisée pour backtest multi-actifs.")

    def log(self, txt, dt=None, symbol=None):
        dt = dt or self.datas[0].datetime.datetime(0)
        symbol = symbol or self.datas[0]._name
        logger.info(f'{dt.isoformat()} - {symbol} - {txt}')

    def notify_order(self, order):
        symbol = order.data._name

        # Ignore until order is completed
        if order.status in [order.Submitted, order.Accepted]:
            return

        # === ORDER COMPLETED ===
        if order.status == order.Completed:
            if order.isbuy():
                self.log(f'BUY EXÉCUTÉ, Prix: {order.executed.price:.2f}, Coût: {order.executed.value:.2f}, Comm: {order.executed.comm:.2f}', symbol=symbol)
                self.entry_prices[symbol] = order.executed.price
                self.active_positions[symbol] = 'long'
                self.open_trades_count += 1

            elif order.issell():
                self.log(f'SELL EXÉCUTÉ, Prix: {order.executed.price:.2f}, Coût: {order.executed.value:.2f}, Comm: {order.executed.comm:.2f}', symbol=symbol)
                self.entry_prices[symbol] = order.executed.price
                self.active_positions[symbol] = 'short'
                self.open_trades_count += 1

            # If no open position after execution, it was a closing order
            if not self.getposition(data=order.data).size:
                self.log('ORDRE DE CLÔTURE EXÉCUTÉ. Position fermée.', symbol=symbol)
                self.active_positions[symbol] = None
                self.entry_prices[symbol] = 0.0
                self.open_trades_count -= 1
                self.cancel_all_pending_orders(symbol)
                self.stop_loss_orders[symbol] = None
                self.take_profit_orders[symbol] = []

        # === ORDER FAILED ===
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log(f'ORDRE ANNULÉ/MARGE/REJETÉ: Type={order.getordername()}, Status={order.getstatusname()}, Info={order.info}', symbol=symbol)
            if order == self.orders.get(symbol):
                self.orders[symbol] = None

        # Clean up order tracking
        self.orders[symbol] = None


    def notify_trade(self, trade):
        if trade.isclosed:
            self.log(f'TRADE CLÔTURÉ: Brute PnL: {trade.pnl:.2f}, Net PnL: {trade.pnlcomm:.2f}', symbol=trade.data._name)

    def cancel_all_pending_orders(self, symbol):
        if self.stop_loss_orders.get(symbol) and self.stop_loss_orders[symbol].status == bt.Order.Submitted:
            self.cancel(self.stop_loss_orders[symbol])
            self.log("Ancien stop-loss annulé.", symbol=symbol)
        for tp_order in self.take_profit_orders.get(symbol, []):
            if tp_order.status == bt.Order.Submitted:
                self.cancel(tp_order)
                self.log("Ancien take-profit annulé.", symbol=symbol)
        self.take_profit_orders[symbol] = []

    def prepare_lstm_input(self, data):
        if len(data) < 100:
            return None
        df = pd.DataFrame({
            'close': data.close.get(size=100),
            'volume': data.volume.get(size=100),
            'RSI': getattr(self, f'rsi_{data._name}').get(size=100),
            'MACD': getattr(self, f'macd_{data._name}').macd.get(size=100),
            'ADX': getattr(self, f'adx_{data._name}').get(size=100)
        })
        df = df.ffill().bfill()
        data_array = df.values.astype(float)
        data_array = np.nan_to_num(data_array, nan=0.0, posinf=0.0, neginf=0.0)
        return data_array.reshape(1, 100, 5)

    def simulate_lstm_prediction(self, data):
        if len(data) < 100:
            return 0.5
        lstm_input = self.prepare_lstm_input(data)
        if lstm_input is None:
            return 0.5
        # Simulation simple : combinaison pondérée des indicateurs
        rsi_val = getattr(self, f'rsi_{data._name}')[0]
        macd_val = getattr(self, f'macd_{data._name}').macd[0]
        macd_signal_val = getattr(self, f'macd_signal_{data._name}')[0]
        adx_val = getattr(self, f'adx_{data._name}')[0]
        prediction = 0.5
        if rsi_val > 55 and macd_val > macd_signal_val and adx_val > 20:
            prediction = 0.7
        elif rsi_val < 45 and macd_val < macd_signal_val and adx_val > 20:
            prediction = 0.3
        return prediction

    def backtest_signal(self, data, action, threshold):
        past_data = pd.DataFrame({
            'close': data.close.get(size=100)
        })
        past_signals = past_data[past_data['close'].shift(1) > threshold if action == 'buy' else past_data['close'].shift(1) < threshold]
        if len(past_signals) < 5:
            self.log(f"[Backtest] Insufficient signals ({len(past_signals)}) for {action}", symbol=data._name)
            return False
        returns = past_signals['close'].pct_change().dropna()
        sharpe_ratio = (returns.mean() / returns.std()) * np.sqrt(252) if returns.std() != 0 else 0
        self.log(f"[Backtest] Sharpe ratio for {action}: {sharpe_ratio:.2f}", symbol=data._name)
        return sharpe_ratio > 0.5

    def calculate_var(self, data, position_size, confidence_level=0.95):
        returns = pd.Series(data.close.get(size=100)).pct_change().dropna()
        mean_return = returns.mean()
        std_return = returns.std()
        var = norm.ppf(1 - confidence_level, mean_return, std_return) * position_size
        return abs(var)

    def calculate_kelly_size(self, data, win_prob, win_loss_ratio):
        if win_prob <= 0 or win_loss_ratio <= 0:
            return 0.0
        kelly = (win_prob - (1 - win_prob) / win_loss_ratio) * self.p.kelly_fraction
        return max(0.0, min(self.p.position_size_ratio, kelly))

    def generate_trading_signal(self, data):
        symbol = data._name
        if len(data) < 100:
            return "hold"

        current_adx = getattr(self, f'adx_{symbol}')[0]
        current_rsi = getattr(self, f'rsi_{symbol}')[0]
        current_macd = getattr(self, f'macd_{symbol}').macd[0]
        current_macd_signal = getattr(self, f'macd_signal_{symbol}')[0]
        current_ema20 = getattr(self, f'ema20_{symbol}')[0]
        current_ema50 = getattr(self, f'ema50_{symbol}')[0]
        current_atr = getattr(self, f'atr_{symbol}')[0]
        current_roc = getattr(self, f'roc_{symbol}')[0] * 100
        current_price = data.close[0]
        current_volume = data.volume[0]

        # Validation du volume
        if current_volume < self.p.min_volume_threshold:
            self.log(f"Volume trop faible ({current_volume:.2f}) pour {symbol}, signal ignoré.", symbol=symbol)
            return "hold"

        # Détection du régime de marché
        strategy_mode = "scalp"
        if current_adx > self.p.adx_trend_threshold:
            strategy_mode = "trend"
        elif current_adx < self.p.adx_range_threshold and self.p.rsi_range_low < current_rsi < self.p.rsi_range_high and current_atr < self.p.atr_range_threshold:
            strategy_mode = "range"

        # Seuil dynamique basé sur l'ADX
        base_up, base_down = {"trend": (0.60, 0.40), "scalp": (0.65, 0.35), "range": (0.55, 0.45)}.get(strategy_mode, (0.55, 0.45))
        adj = (current_adx / 1000)
        dynamic_up = max(base_up - adj, base_down + 0.05)
        dynamic_down = min(base_down + adj, dynamic_up - 0.05)

        # Prédiction LSTM simulée
        prediction = self.simulate_lstm_prediction(data)

        # Conditions de signal
        trend_up = current_ema20 > current_ema50
        trend_down = current_ema20 < current_ema50
        macd_bullish = current_macd > current_macd_signal
        rsi_strong = (trend_up and current_rsi > 50) or (trend_down and current_rsi < 45) or (abs(current_rsi - 50) > 15)
        rolling_high = max(data.high.get(size=20))
        rolling_low = min(data.low.get(size=20))
        breakout_up = current_price > (rolling_high - 0.1 * current_atr)
        breakout_down = current_price < (rolling_low + 0.1 * current_atr)
        bullish_divergence = (data.close[0] < data.close[-3] and current_rsi > getattr(self, f'rsi_{symbol}')[-3])
        bearish_divergence = (data.close[0] > data.close[-3] and current_rsi < getattr(self, f'rsi_{symbol}')[-3])

        confidence_factors = []
        if prediction > 0.55 or prediction < 0.45:
            confidence_factors.append("LSTM strong")
        if (trend_up and current_macd > current_macd_signal) or (trend_down and current_macd < current_macd_signal):
            confidence_factors.append("MACD aligned")
        if current_rsi > 50 or current_rsi < 50:
            confidence_factors.append("RSI strong")
        if (trend_up and current_ema20 > current_ema50) or (trend_down and current_ema20 < current_ema50):
            confidence_factors.append("EMA trend")
        if abs(current_roc) > 0.5:
            confidence_factors.append("ROC momentum")
        if breakout_up or breakout_down:
            confidence_factors.append("Breakout detected")

        self.log(f"Indicateurs: RSI={current_rsi:.2f}, MACD={current_macd:.4f}, ADX={current_adx:.2f}, ROC={current_roc:.2f}, Mode={strategy_mode}, Confiance={len(confidence_factors)}/6", symbol=symbol)

        # Validation du signal
        action = "hold"
        if len(confidence_factors) < 3:
            self.log(f"Signal rejeté: Confiance insuffisante ({len(confidence_factors)}/6)", symbol=symbol)
            return action

        if strategy_mode == "scalp" and rsi_strong:
            if prediction > dynamic_up and current_roc > 0.5 and self.backtest_signal(data, "buy", dynamic_up):
                action = "buy"
            elif prediction < dynamic_down and current_roc < -0.5 and self.backtest_signal(data, "sell", dynamic_down):
                action = "sell"
        elif strategy_mode == "trend":
            if trend_up and macd_bullish and prediction > dynamic_up and current_roc > 0.5 and self.backtest_signal(data, "buy", dynamic_up):
                action = "buy"
            elif trend_down and not macd_bullish and prediction < dynamic_down and current_roc < -0.5 and self.backtest_signal(data, "sell", dynamic_down):
                action = "sell"
        elif strategy_mode == "range":
            if current_price <= (rolling_low + 0.1 * current_atr) and prediction > dynamic_up and current_roc > 0.3 and self.backtest_signal(data, "buy", dynamic_up):
                action = "buy"
            elif current_price >= (rolling_high - 0.1 * current_atr) and prediction < dynamic_down and current_roc < -0.3 and self.backtest_signal(data, "sell", dynamic_down):
                action = "sell"

        if bullish_divergence and prediction > 0.65 and current_roc > 0.5 and self.backtest_signal(data, "buy", dynamic_up):
            action = "buy"
        elif bearish_divergence and prediction < 0.35 and current_roc < -0.5 and self.backtest_signal(data, "sell", dynamic_down):
            action = "sell"

        if self.active_positions[symbol] == "long" and (trend_down or not macd_bullish or current_roc < -0.5):
            action = "close_buy"
        elif self.active_positions[symbol] == "short" and (trend_up or macd_bullish or current_roc > 0.5):
            action = "close_sell"

        if action == self.last_signal.get(symbol):
            self.log(f"Signal {action} ignoré (répétition).", symbol=symbol)
            return "hold"

        return action

    def next(self):
        for data in self.datas:
            symbol = data._name
            if self.open_trades_count >= self.p.max_concurrent_trades and not self.active_positions[symbol]:
                self.log(f"Max trades atteint ({self.open_trades_count}/{self.p.max_concurrent_trades}), pas de nouveau trade pour {symbol}.", symbol=symbol)
                continue

            action = self.generate_trading_signal(data)
            current_price = data.close[0]
            current_atr = getattr(self, f'atr_{symbol}')[0]
            current_adx = getattr(self, f'adx_{symbol}')[0]

            if action == "hold":
                self.update_trailing_stop(data, symbol, current_price, current_atr, current_adx)
                continue

            # Calcul de la taille de la position
            capital = self.broker.getcash()
            position_value = capital * self.p.position_size_ratio / self.p.max_concurrent_trades
            var = self.calculate_var(data, position_value, self.p.var_confidence_level)
            win_prob = 0.6 if action == "buy" else 0.4  # Simplification
            win_loss_ratio = 2.0  # Simplification
            kelly_size = self.calculate_kelly_size(data, win_prob, win_loss_ratio)
            size = min(position_value / current_price, (capital * kelly_size) / current_price)
            size = max(0, size / self.p.leverage)

            self.cancel_all_pending_orders(symbol)

            if action == "buy":
                self.log(f"Signal BUY détecté, taille: {size:.4f}", symbol=symbol)
                self.orders[symbol] = self.buy(data=data, size=size, exectype=bt.Order.Market)
                sl_price = current_price * (1 - self.p.initial_stop_loss_pct)
                self.stop_loss_orders[symbol] = self.sell(data=data, size=size, exectype=bt.Order.Stop, price=sl_price)
                for i, tp_multiple in enumerate(self.p.take_profit_multiples):
                    tp_size = size / len(self.p.take_profit_multiples)
                    tp_price = current_price * (1 + tp_multiple * current_atr / current_price)
                    tp_order = self.sell(data=data, size=tp_size, exectype=bt.Order.Limit, price=tp_price)
                    self.take_profit_orders[symbol].append(tp_order)
                    self.log(f"Take-Profit {i+1} placé à {tp_price:.2f} pour {tp_size:.4f}", symbol=symbol)
            elif action == "sell":
                self.log(f"Signal SELL détecté, taille: {size:.4f}", symbol=symbol)
                self.orders[symbol] = self.sell(data=data, size=size, exectype=bt.Order.Market)
                sl_price = current_price * (1 + self.p.initial_stop_loss_pct)
                self.stop_loss_orders[symbol] = self.buy(data=data, size=size, exectype=bt.Order.Stop, price=sl_price)
                for i, tp_multiple in enumerate(self.p.take_profit_multiples):
                    tp_size = size / len(self.p.take_profit_multiples)
                    tp_price = current_price * (1 - tp_multiple * current_atr / current_price)
                    tp_order = self.buy(data=data, size=tp_size, exectype=bt.Order.Limit, price=tp_price)
                    self.take_profit_orders[symbol].append(tp_order)
                    self.log(f"Take-Profit {i+1} placé à {tp_price:.2f} pour {tp_size:.4f}", symbol=symbol)
            elif action in ["close_buy", "close_sell"]:
                position = self.getposition(data=data)
                if position.size != 0:
                    self.orders[symbol] = self.close(data=data, exectype=bt.Order.Market)
                    self.log(f"Clôture de la position ({action})", symbol=symbol)

            self.last_signal[symbol] = action

    def update_trailing_stop(self, data, symbol, current_price, current_atr, current_adx):
        position = self.getposition(data=data)
        if position.size == 0:
            return

        remaining_size = abs(position.size)
        if len(self.take_profit_orders[symbol]) > 0:
            closed_tp = sum(tp.size for tp in self.take_profit_orders[symbol] if tp.status != bt.Order.Submitted)
            remaining_size = max(0, abs(position.size) - closed_tp)

        if remaining_size == 0:
            return

        dynamic_trailing_distance = current_atr * self.p.trailing_atr_multiplier
        if current_adx > self.p.adx_trend_threshold:
            dynamic_trailing_distance *= 0.8
        elif current_adx < self.p.adx_range_threshold:
            dynamic_trailing_distance *= 1.2

        trailing_pct = dynamic_trailing_distance / current_price
        trailing_pct = max(self.p.trailing_min_pct, min(self.p.trailing_max_pct, trailing_pct))
        calculated_trailing_price = current_price * trailing_pct

        new_sl_price = 0.0
        if position.size > 0:  # Long
            new_sl_price = max(self.entry_prices[symbol] * (1 - self.p.initial_stop_loss_pct), current_price - calculated_trailing_price)
            if self.stop_loss_orders[symbol] and self.stop_loss_orders[symbol].price > 0:
                new_sl_price = max(new_sl_price, self.stop_loss_orders[symbol].price)
        elif position.size < 0:  # Short
            new_sl_price = min(self.entry_prices[symbol] * (1 + self.p.initial_stop_loss_pct), current_price + calculated_trailing_price)
            if self.stop_loss_orders[symbol] and self.stop_loss_orders[symbol].price > 0:
                new_sl_price = min(new_sl_price, self.stop_loss_orders[symbol].price)

        if new_sl_price > 0 and (self.stop_loss_orders[symbol] is None or
                                 abs(new_sl_price - self.stop_loss_orders[symbol].price) / self.stop_loss_orders[symbol].price > 0.0001):
            self.cancel(self.stop_loss_orders[symbol])
            self.log(f"Mise à jour du Trailing SL: {new_sl_price:.2f} pour {remaining_size:.4f}", symbol=symbol)
            self.stop_loss_orders[symbol] = self.close(data=data, exectype=bt.Order.Stop, price=new_sl_price, size=remaining_size)

def run_backtest():
    cerebro = bt.Cerebro()

    # Ajout de la stratégie
    cerebro.addstrategy(MyTradingStrategy)

    # Chargement des données historiques
    try:
        df = pd.read_csv('./backtest/data.csv')
        required_columns = ['datetime', 'open', 'high', 'low', 'close', 'volume']
        if not all(col in df.columns for col in required_columns):
            raise ValueError(f"Le fichier CSV doit contenir les colonnes: {required_columns}")
        df['datetime'] = pd.to_datetime(df['datetime'])
        df = df.ffill().bfill()
        df.to_csv('data_cleaned.csv', index=False)
        
        data = bt.feeds.PandasData(
            dataname=df,
            datetime='datetime',
            open='open',
            high='high',
            low='low',
            close='close',
            volume='volume',
            timeframe=bt.TimeFrame.Minutes,
            compression=1,
            name='asset1'
        )
        cerebro.adddata(data)
    except Exception as e:
        logger.error(f"Erreur lors du chargement des données: {e}")
        return

    # Configuration du broker
    start_cash = 10000.0
    cerebro.broker.setcash(start_cash)
    cerebro.broker.setcommission(commission=0.0004)
    cerebro.broker.set_slippage_perc(perc=0.0001)

    # Analyseurs de performance
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe', timeframe=bt.TimeFrame.Days)
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='trades')
    cerebro.addanalyzer(bt.analyzers.SQN, _name='sqn')

    # Indicateurs pour le graphique
    cerebro.addindicator(btind.RSI, period=14)
    cerebro.addindicator(btind.MACD)
    cerebro.addindicator(btind.EMA, period=20)
    cerebro.addindicator(btind.EMA, period=50)

    # Exécution du backtest
    logger.info(f'Capital initial: {cerebro.broker.getvalue():.2f}')
    results = cerebro.run()
    strategy = results[0]

    # Affichage des résultats
    final_value = cerebro.broker.getvalue()
    logger.info(f'Capital final: {final_value:.2f}')
    logger.info(f'Profit: {final_value - start_cash:.2f}')

    if 'sharpe' in strategy.analyzers:
        sharpe_ratio = strategy.analyzers.sharpe.get_analysis()
        logger.info(f'Sharpe Ratio: {sharpe_ratio.get("sharperatio", "N/A"):.2f}')
    if 'drawdown' in strategy.analyzers:
        drawdown_analysis = strategy.analyzers.drawdown.get_analysis()
        logger.info(f'Max Drawdown: {drawdown_analysis.get("max", {}).get("drawdown", 0.0):.2f}%')
        logger.info(f'Longest Drawdown Duration: {drawdown_analysis.get("max", {}).get("len", 0)} bars')
    if 'trades' in strategy.analyzers:
        trades_analysis = strategy.analyzers.trades.get_analysis()
        logger.info(f'Total Trades: {trades_analysis.total.opened}')
        logger.info(f'Winning Trades: {trades_analysis.won.total}')
        logger.info(f'Losing Trades: {trades_analysis.lost.total}')
        if trades_analysis.won.total > 0:
            logger.info(f'Avg Win: {trades_analysis.won.pnl.average:.2f}')
        if trades_analysis.lost.total > 0:
            logger.info(f'Avg Loss: {trades_analysis.lost.pnl.average:.2f}')
        if trades_analysis.won.total > 0 and trades_analysis.lost.total > 0:
            logger.info(f'Win Rate: {(trades_analysis.won.total / trades_analysis.total.opened * 100):.2f}%')
            logger.info(f'Avg Win/Avg Loss Ratio: {(trades_analysis.won.pnl.average / abs(trades_analysis.lost.pnl.average)):.2f}')
    if 'sqn' in strategy.analyzers:
        sqn_value = strategy.analyzers.sqn.get_analysis()
        logger.info(f'System Quality Number (SQN): {sqn_value.get("sqn", "N/A"):.2f}')

    # Génération du graphique
    try:
        logger.info("Génération du graphique de performance...")
        cerebro.plot(style='candlestick', iplot=False, numfigs=1, savefig=True, figpath='backtest_result.png')
        logger.info("Graphique sauvegardé sous 'backtest_result.png'.")
    except Exception as e:
        logger.error(f"Erreur lors de la génération du graphique: {e}")

if __name__ == '__main__':
    run_backtest()