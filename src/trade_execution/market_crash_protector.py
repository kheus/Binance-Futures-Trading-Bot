class MarketCrashProtector:
    def __init__(self, threshold=0.05, lookback=5):
        self.threshold = threshold
        self.lookback = lookback
        self.price_buffer = {}

    def check_market_crash(self, symbol, new_price):
        if symbol not in self.price_buffer:
            self.price_buffer[symbol] = []
        self.price_buffer[symbol].append(new_price)
        if len(self.price_buffer[symbol]) > self.lookback:
            self.price_buffer[symbol].pop(0)
        if len(self.price_buffer[symbol]) == self.lookback:
            initial = self.price_buffer[symbol][0]
            change = (new_price - initial) / initial
            return change <= -self.threshold
        return False