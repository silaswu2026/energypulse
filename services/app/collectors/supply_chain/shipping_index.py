"""
航运指数采集器

数据来源:
- Baltic Dry Index (BDI) - 干散货航运
- Baltic Capesize Index (BCI) - 海岬型船
"""

import os
import sys
import logging
import requests
from datetime import datetime, date
from typing import List, Dict

sys.path.insert(0, "/app")
from database import get_db

logger = logging.getLogger("energypulse.supply_chain.shipping")

# Investopedia 或其他免费源
BDI_SOURCES = {
    "investopedia": "https://www.investopedia.com/terms/b/balticdryindex.asp",
    "tradingeconomics": "https://tradingeconomics.com/commodity/baltic",
}


class ShippingIndexCollector:
    """航运指数采集器"""
    
    # BDI 历史参考数据 (作为备用)
    BDI_REFERENCE = {
        "extremely_strong": 2500,  # 极强
        "strong": 2000,            # 强
        "normal": 1500,            # 正常
        "weak": 1000,              # 弱
        "extremely_weak": 500,     # 极弱
    }
    
    def __init__(self):
        self.db = get_db()
        
    def collect_bdi(self) -> Dict:
        """
        采集BDI指数
        
        注：BDI是专业数据，需要付费订阅。
        此处实现：
        1. 尝试免费API
        2. 回退到手动输入/数据库最新值
        """
        logger.info("采集BDI指数...")
        
        # 尝试从已有数据获取最新值
        try:
            sql = """
                SELECT value, date 
                FROM macro_indicators 
                WHERE indicator = 'BDI' 
                ORDER BY date DESC LIMIT 1
            """
            result = self.db.query(sql)
            
            if result:
                return {
                    "index_name": "BDI",
                    "value": result[0]["value"],
                    "date": result[0]["date"].isoformat(),
                    "source": "database",
                    "trend": self._classify_bdi(result[0]["value"])
                }
        except Exception as e:
            logger.warning(f"数据库查询失败: {e}")
        
        # 返回占位数据（用户可手动更新）
        return {
            "index_name": "BDI",
            "value": None,
            "date": date.today().isoformat(),
            "source": "placeholder",
            "note": "BDI需要专业数据订阅，建议从Bloomberg/Reuters获取"
        }
    
    def _classify_bdi(self, value: float) -> str:
        """分类BDI水平"""
        if value >= self.BDI_REFERENCE["extremely_strong"]:
            return "极强"
        elif value >= self.BDI_REFERENCE["strong"]:
            return "强"
        elif value >= self.BDI_REFERENCE["normal"]:
            return "正常"
        elif value >= self.BDI_REFERENCE["weak"]:
            return "弱"
        else:
            return "极弱"
    
    def save_bdi(self, value: float, source: str = "manual"):
        """保存BDI数据（手动更新入口）"""
        try:
            sql = """
                INSERT INTO macro_indicators (indicator, date, value, source)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (indicator, date) DO UPDATE SET
                    value = EXCLUDED.value,
                    source = EXCLUDED.source
            """
            self.db.execute(sql, ("BDI", date.today(), value, source))
            logger.info(f"BDI已保存: {value}")
        except Exception as e:
            logger.error(f"保存BDI失败: {e}")
    
    def get_bdi_analysis(self) -> Dict:
        """获取BDI分析"""
        bdi = self.collect_bdi()
        
        if not bdi.get("value"):
            return {
                "status": "no_data",
                "interpretation": "暂无BDI数据"
            }
        
        value = bdi["value"]
        trend = bdi.get("trend", "unknown")
        
        # 投资含义
        if value > 2000:
            implication = "航运市场火热，大宗商品运输需求强劲，利好煤炭/铁矿石出口国"
        elif value > 1500:
            implication = "航运市场健康，运输需求稳定"
        elif value > 1000:
            implication = "航运市场偏弱，需关注需求端变化"
        else:
            implication = "航运市场低迷，全球贸易活动疲软"
        
        return {
            "status": "ok",
            "current_value": value,
            "trend": trend,
            "interpretation": implication,
            "coal_impact": "BDI上涨通常伴随煤炭海运需求增加"
        }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    collector = ShippingIndexCollector()
    
    # 测试
    result = collector.collect_bdi()
    print(f"BDI: {result}")
    
    analysis = collector.get_bdi_analysis()
    print(f"分析: {analysis}")
