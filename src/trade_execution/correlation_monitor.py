import numpy as np
import pandas as pd

class CorrelationMonitor:
    def __init__(self, symbols):
        self.symbols = symbols
        self.price_history = {s: [] for s in symbols}

    def update_prices(self, symbol, price):
        self.price_history[symbol].append(price)
        if len(self.price_history[symbol]) > 100:
            self.price_history[symbol].pop(0)

    def get_correlation_matrix(self):
        closes = []
        for sym in self.symbols:
            if len(self.price_history[sym]) >= 100:
                closes.append(self.price_history[sym][-100:])
        if len(closes) >= 3:
            corr_matrix = np.corrcoef(closes)
            return pd.DataFrame(corr_matrix, index=self.symbols, columns=self.symbols)
        return None