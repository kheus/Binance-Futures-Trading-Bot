import logging
import pandas as pd
from datetime import datetime, timedelta
from src.database.db_handler import execute_query

logger = logging.getLogger(__name__)

class TradeAnalyzer:
    def __init__(self):
        self.trade_history = []

    def add_trade(self, trade_details):
        try:
            if not all(key in trade_details for key in ["order_id", "symbol", "side", "price", "quantity", "timestamp", "pnl"]):
                logger.error(f"[TradeAnalyzer] Invalid trade details: {trade_details}")
                return
            self.trade_history.append(trade_details)
            self.analyze_last_trade()
        except Exception as e:
            logger.error(f"[TradeAnalyzer] Error adding trade: {e}")

    def analyze_last_trade(self):
        if not self.trade_history:
            logger.warning("[TradeAnalyzer] No trades to analyze")
            return
        try:
            trade = self.trade_history[-1]
            entry_time = datetime.fromtimestamp(trade["timestamp"] / 1000)
            exit_time = trade.get("exit_time", trade["timestamp"])  # Fallback to entry time if exit_time missing
            analysis = {
                "order_id": trade["order_id"],
                "symbol": trade["symbol"],
                "duration": (exit_time - entry_time).total_seconds() / 60.0 if isinstance(exit_time, datetime) else 0.0,
                "risk_reward": self.calculate_risk_reward(trade),
                "volatility": self.get_trade_volatility(trade),
                "performance": "good" if trade["pnl"] > 0 else "bad"
            }
            self.save_analysis(trade["symbol"], analysis)
        except Exception as e:
            logger.error(f"[TradeAnalyzer] Error analyzing trade {trade.get('order_id', 'unknown')}: {e}")

    def calculate_risk_reward(self, trade):
        try:
            if trade.get("stop_loss") and trade.get("take_profit") and trade.get("price"):
                entry_price = float(trade["price"])
                stop_loss = float(trade["stop_loss"])
                take_profit = float(trade["take_profit"])
                risk = abs(entry_price - stop_loss)
                reward = abs(take_profit - entry_price)
                return reward / risk if risk > 0 else 0.0
            return 0.0
        except Exception as e:
            logger.error(f"[TradeAnalyzer] Error calculating risk/reward: {e}")
            return 0.0

    def get_trade_volatility(self, trade):
        try:
            symbol = trade["symbol"]
            timestamp = trade["timestamp"]
            query = """
                SELECT high, low
                FROM price_data
                WHERE symbol = %s AND timestamp <= %s
                ORDER BY timestamp DESC
                LIMIT 20
            """
            result = execute_query(query, (symbol, timestamp), fetch=True)
            if not result:
                return 0.0
            highs = [float(row[0]) for row in result]
            lows = [float(row[1]) for row in result]
            ranges = [h - l for h, l in zip(highs, lows)]
            return float(pd.Series(ranges).mean()) if ranges else 0.0
        except Exception as e:
            logger.error(f"[TradeAnalyzer] Error calculating volatility: {e}")
            return 0.0

    def save_analysis(self, symbol, analysis):
        try:
            query = """
                INSERT INTO trade_analysis (order_id, symbol, duration, risk_reward, volatility, performance, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            params = (
                analysis["order_id"],
                symbol,
                analysis["duration"],
                analysis["risk_reward"],
                analysis["volatility"],
                analysis["performance"],
                int(datetime.now().timestamp() * 1000)
            )
            execute_query(query, params)
            logger.info(f"[TradeAnalyzer] Saved analysis for {symbol}: {analysis}")
        except Exception as e:
            logger.error(f"[TradeAnalyzer] Error saving analysis for {symbol}: {e}")

    def generate_daily_report(self):
        try:
            today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            start_time = int(today.timestamp() * 1000)
            end_time = int((today + timedelta(days=1)).timestamp() * 1000)

            # Fetch trades from database for today
            query = """
                SELECT symbol, side, price, quantity, pnl, timestamp
                FROM trades
                WHERE timestamp >= %s AND timestamp < %s
            """
            trades = execute_query(query, (start_time, end_time), fetch=True)
            if not trades:
                logger.info("[TradeAnalyzer] No trades for daily report")
                return {"date": today.strftime("%Y-%m-%d"), "trades": 0, "win_rate": 0.0, "total_pnl": 0.0}

            df = pd.DataFrame(trades, columns=["symbol", "side", "price", "quantity", "pnl", "timestamp"])
            total_trades = len(df)
            wins = len(df[df["pnl"] > 0])
            win_rate = (wins / total_trades * 100) if total_trades > 0 else 0.0
            total_pnl = float(df["pnl"].sum())
            avg_duration = float(df["timestamp"].diff().mean()) / 1000 / 60 if len(df) > 1 else 0.0

            # Group by symbol
            symbol_summary = df.groupby("symbol").agg({
                "pnl": "sum",
                "side": "count"
            }).rename(columns={"side": "trade_count"}).to_dict()

            report = {
                "date": today.strftime("%Y-%m-%d"),
                "trades": total_trades,
                "win_rate": round(win_rate, 2),
                "total_pnl": round(total_pnl, 2),
                "avg_duration_minutes": round(avg_duration, 2),
                "symbol_summary": symbol_summary
            }

            # Save report to database
            query = """
                INSERT INTO daily_reports (report_date, total_trades, win_rate, total_pnl, avg_duration, symbol_summary)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            params = (
                report["date"],
                report["trades"],
                report["win_rate"],
                report["total_pnl"],
                report["avg_duration_minutes"],
                str(symbol_summary)
            )
            execute_query(query, params)
            logger.info(f"[TradeAnalyzer] Generated daily report: {report}")
            return report
        except Exception as e:
            logger.error(f"[TradeAnalyzer] Error generating daily report: {e}")
            return None