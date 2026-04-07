"""
煤炭个股采集器

覆盖主要煤炭上市公司：
- 中国神华 (601088.SH)
- 陕西煤业 (601225.SH)
- 中煤能源 (601898.SH)
- 兖矿能源 (600188.SH)
- 潞安环能 (601699.SH)
- 山西焦煤 (000983.SZ)
"""

import os
import sys
import logging
from datetime import datetime, date
from typing import Dict, List, Optional
from dataclasses import dataclass

sys.path.insert(0, "/app")
from database import get_db

logger = logging.getLogger("energypulse.coal_stocks")


@dataclass
class CoalStock:
    """煤炭个股数据"""
    symbol: str
    name: str
    sector: str  # 动力煤/焦煤/综合
    close_price: float
    change_pct: float
    pe_ttm: Optional[float]
    pb: Optional[float]
    dividend_yield: Optional[float]
    market_cap: Optional[float]


# 重点煤炭股配置
KEY_COAL_STOCKS = {
    "601088.SH": {"name": "中国神华", "sector": "综合", "type": "thermal"},
    "601225.SH": {"name": "陕西煤业", "sector": "动力煤", "type": "thermal"},
    "601898.SH": {"name": "中煤能源", "sector": "综合", "type": "mixed"},
    "600188.SH": {"name": "兖矿能源", "sector": "综合", "type": "mixed"},
    "601699.SH": {"name": "潞安环能", "sector": "喷吹煤", "type": "pci"},
    "000983.SZ": {"name": "山西焦煤", "sector": "焦煤", "type": "coking"},
    "600123.SH": {"name": "兰花科创", "sector": "无烟煤", "type": "anthracite"},
    "600348.SH": {"name": "阳泉煤业", "sector": "无烟煤", "type": "anthracite"},
}


class CoalStockCollector:
    """煤炭个股采集器"""
    
    def __init__(self):
        self.db = get_db()
        self.stocks_config = KEY_COAL_STOCKS
    
    def get_sector_performance(self) -> Dict:
        """获取各细分板块表现"""
        performance = {}
        
        for sector in ["动力煤", "焦煤", "无烟煤", "综合"]:
            try:
                symbols = [s for s, info in self.stocks_config.items() 
                          if info["sector"] == sector]
                
                if not symbols:
                    continue
                
                # 计算板块平均涨跌幅
                placeholders = ','.join(['%s'] * len(symbols))
                sql = f"""
                    SELECT AVG(change_pct) as avg_change,
                           COUNT(*) as count
                    FROM stock_daily
                    WHERE symbol IN ({placeholders})
                    AND trade_date = (
                        SELECT MAX(trade_date) FROM stock_daily 
                        WHERE symbol IN ({placeholders})
                    )
                """
                params = symbols + symbols
                result = self.db.query(sql, params)
                
                if result:
                    performance[sector] = {
                        "avg_change": round(result[0]["avg_change"] or 0, 2),
                        "stock_count": result[0]["count"],
                        "leading": None  # 领涨股稍后填充
                    }
            
            except Exception as e:
                logger.error(f"获取{sector}板块表现失败: {e}")
        
        return performance
    
    def get_dividend_leaders(self, top_n: int = 5) -> List[Dict]:
        """获取股息率领先个股"""
        try:
            symbols = list(self.stocks_config.keys())
            placeholders = ','.join(['%s'] * len(symbols))
            
            sql = f"""
                SELECT symbol, close_price, pe_ratio, pb_ratio,
                       dividend_yield_ttm, market_cap
                FROM stock_daily
                WHERE symbol IN ({placeholders})
                AND trade_date = (
                    SELECT MAX(trade_date) FROM stock_daily 
                    WHERE symbol IN ({placeholders})
                )
                AND dividend_yield_ttm IS NOT NULL
                ORDER BY dividend_yield_ttm DESC
                LIMIT %s
            """
            params = symbols + symbols + (top_n,)
            results = self.db.query(sql, params)
            
            return [
                {
                    "symbol": r["symbol"],
                    "name": self.stocks_config.get(r["symbol"], {}).get("name", ""),
                    "sector": self.stocks_config.get(r["symbol"], {}).get("sector", ""),
                    "dividend_yield": r["dividend_yield_ttm"],
                    "pe_ttm": r["pe_ratio"],
                    "market_cap": r["market_cap"],
                }
                for r in results
            ]
        
        except Exception as e:
            logger.error(f"获取股息领先股失败: {e}")
            return []
    
    def get_value_screening(self) -> Dict:
        """价值股筛选"""
        try:
            symbols = list(self.stocks_config.keys())
            placeholders = ','.join(['%s'] * len(symbols))
            
            # 低PE筛选
            sql_pe = f"""
                SELECT symbol, pe_ratio, pb_ratio, dividend_yield_ttm
                FROM stock_daily
                WHERE symbol IN ({placeholders})
                AND trade_date = (
                    SELECT MAX(trade_date) FROM stock_daily
                    WHERE symbol IN ({placeholders})
                )
                AND pe_ratio > 0 AND pe_ratio < 10
                ORDER BY pe_ratio ASC
            """
            results = self.db.query(sql_pe, symbols + symbols)
            
            low_pe_stocks = [
                {
                    "symbol": r["symbol"],
                    "name": self.stocks_config.get(r["symbol"], {}).get("name", ""),
                    "pe": r["pe_ratio"],
                    "pb": r["pb_ratio"],
                    "dividend": r["dividend_yield_ttm"]
                }
                for r in results[:5]
            ]
            
            # 高股息筛选(>5%)
            sql_div = f"""
                SELECT symbol, dividend_yield_ttm, pe_ratio
                FROM stock_daily
                WHERE symbol IN ({placeholders})
                AND trade_date = (
                    SELECT MAX(trade_date) FROM stock_daily
                    WHERE symbol IN ({placeholders})
                )
                AND dividend_yield_ttm > 5.0
                ORDER BY dividend_yield_ttm DESC
            """
            results_div = self.db.query(sql_div, symbols + symbols)
            
            high_div_stocks = [
                {
                    "symbol": r["symbol"],
                    "name": self.stocks_config.get(r["symbol"], {}).get("name", ""),
                    "dividend": r["dividend_yield_ttm"],
                    "pe": r["pe_ratio"]
                }
                for r in results_div
            ]
            
            return {
                "low_pe": low_pe_stocks,
                "high_dividend": high_div_stocks
            }
        
        except Exception as e:
            logger.error(f"价值筛选失败: {e}")
            return {"low_pe": [], "high_dividend": []}
    
    def generate_sector_report(self) -> Dict:
        """生成煤炭板块报告"""
        sector_perf = self.get_sector_performance()
        div_leaders = self.get_dividend_leaders(5)
        value_screen = self.get_value_screening()
        
        return {
            "sector_performance": sector_perf,
            "dividend_leaders": div_leaders,
            "value_screening": value_screen,
            "investment_themes": self._generate_themes(sector_perf, div_leaders),
            "timestamp": datetime.utcnow().isoformat()
        }
    
    def _generate_themes(self, sector_perf, div_leaders) -> List[str]:
        """生成投资主题"""
        themes = []
        
        # 高股息主题
        if div_leaders and div_leaders[0].get("dividend_yield", 0) > 6:
            themes.append(f"高股息策略：{div_leaders[0]['name']}股息率{div_leaders[0]['dividend_yield']:.1f}%")
        
        # 板块轮动
        if sector_perf.get("焦煤", {}).get("avg_change", 0) > sector_perf.get("动力煤", {}).get("avg_change", 0):
            themes.append("焦煤板块跑赢，钢铁需求预期改善")
        
        # 价值重估
        low_pe_count = len([s for s in div_leaders if s.get("pe_ttm", 99) < 8])
        if low_pe_count >= 2:
            themes.append(f"板块估值偏低，{low_pe_count}只个股PE<8，关注价值重估机会")
        
        return themes


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    collector = CoalStockCollector()
    report = collector.generate_sector_report()
    print(json.dumps(report, indent=2, ensure_ascii=False, default=str))
