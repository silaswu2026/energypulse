"""
技术指标计算处理器
计算RSI、MACD、移动平均线
"""

import logging
import numpy as np
from datetime import date, timedelta
from database import get_db

logger = logging.getLogger("energypulse.technical")


class TechnicalProcessor:
    """技术指标计算器"""

    def __init__(self):
        self.db = get_db()

    def calculate_all(self):
        """计算所有技术指标"""
        logger.info("开始计算技术指标...")
        
        # 美股技术指标
        self._calculate_for_market("US")
        
        # A股技术指标
        self._calculate_for_market("CN")
        
        logger.info("技术指标计算完成")

    def _calculate_for_market(self, market: str):
        """为指定市场计算指标"""
        # 获取所有股票代码
        symbols = self.db.query(
            "SELECT DISTINCT symbol FROM stock_daily WHERE market = %s ORDER BY symbol",
            [market]
        )
        
        for row in symbols:
            symbol = row["symbol"]
            self._calculate_for_symbol(market, symbol)

    def _calculate_for_symbol(self, market: str, symbol: str):
        """为单只股票计算指标"""
        # 获取历史价格数据（最近120天）
        prices = self.db.query(
            """SELECT trade_date, close, high, low 
               FROM stock_daily 
               WHERE market = %s AND symbol = %s 
               ORDER BY trade_date DESC 
               LIMIT 120""",
            [market, symbol]
        )
        
        if len(prices) < 30:  # 需要至少30天数据
            return
        
        # 按日期正序排列用于计算
        prices.reverse()
        closes = np.array([p["close"] for p in prices])
        highs = np.array([p["high"] for p in prices])
        lows = np.array([p["low"] for p in prices])
        
        # 计算指标
        ma5 = self._calculate_ma(closes, 5)
        ma20 = self._calculate_ma(closes, 20)
        ma60 = self._calculate_ma(closes, 60)
        rsi14 = self._calculate_rsi(closes, 14)
        macd, macd_signal, macd_hist = self._calculate_macd(closes)
        
        # 更新最新一天的数据
        latest_idx = -1
        latest_date = prices[latest_idx]["trade_date"]
        
        self.db.execute(
            """UPDATE stock_daily 
               SET ma5 = %s, ma20 = %s, ma60 = %s, 
                   rsi14 = %s, macd = %s, macd_signal = %s, macd_hist = %s
               WHERE trade_date = %s AND market = %s AND symbol = %s""",
            [
                ma5[latest_idx] if not np.isnan(ma5[latest_idx]) else None,
                ma20[latest_idx] if not np.isnan(ma20[latest_idx]) else None,
                ma60[latest_idx] if not np.isnan(ma60[latest_idx]) else None,
                rsi14[latest_idx] if not np.isnan(rsi14[latest_idx]) else None,
                macd[latest_idx] if not np.isnan(macd[latest_idx]) else None,
                macd_signal[latest_idx] if not np.isnan(macd_signal[latest_idx]) else None,
                macd_hist[latest_idx] if not np.isnan(macd_hist[latest_idx]) else None,
                latest_date, market, symbol
            ]
        )
        
        logger.debug(f"{market}/{symbol} 技术指标已更新")

    def _calculate_ma(self, prices: np.ndarray, period: int) -> np.ndarray:
        """计算移动平均线"""
        ma = np.full_like(prices, np.nan)
        for i in range(period - 1, len(prices)):
            ma[i] = np.mean(prices[i - period + 1:i + 1])
        return ma

    def _calculate_rsi(self, prices: np.ndarray, period: int = 14) -> np.ndarray:
        """计算RSI"""
        deltas = np.diff(prices)
        gains = np.where(deltas > 0, deltas, 0)
        losses = np.where(deltas < 0, -deltas, 0)
        
        avg_gains = np.full_like(prices, np.nan)
        avg_losses = np.full_like(prices, np.nan)
        
        # 初始平均值
        avg_gains[period] = np.mean(gains[:period])
        avg_losses[period] = np.mean(losses[:period])
        
        # 平滑移动平均
        for i in range(period + 1, len(prices)):
            avg_gains[i] = (avg_gains[i-1] * (period - 1) + gains[i-1]) / period
            avg_losses[i] = (avg_losses[i-1] * (period - 1) + losses[i-1]) / period
        
        rs = avg_gains / avg_losses
        rsi = 100 - (100 / (1 + rs))
        return rsi

    def _calculate_macd(self, prices: np.ndarray, 
                        fast: int = 12, slow: int = 26, signal: int = 9):
        """计算MACD"""
        ema_fast = self._calculate_ema(prices, fast)
        ema_slow = self._calculate_ema(prices, slow)
        
        macd = ema_fast - ema_slow
        macd_signal = self._calculate_ema(macd, signal)
        macd_hist = macd - macd_signal
        
        return macd, macd_signal, macd_hist

    def _calculate_ema(self, prices: np.ndarray, period: int) -> np.ndarray:
        """计算EMA"""
        ema = np.full_like(prices, np.nan)
        multiplier = 2 / (period + 1)
        
        # 初始值为SMA
        ema[period - 1] = np.mean(prices[:period])
        
        for i in range(period, len(prices)):
            ema[i] = (prices[i] - ema[i-1]) * multiplier + ema[i-1]
        
        return ema


if __name__ == "__main__":
    processor = TechnicalProcessor()
    processor.calculate_all()
    print("技术指标计算完成")
