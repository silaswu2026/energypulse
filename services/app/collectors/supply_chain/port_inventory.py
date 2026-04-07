"""
港口库存采集器

覆盖港口：
- 秦皇岛港 (中国最重要煤炭下水港)
- 曹妃甸港
- 黄骅港
- 纽卡斯尔港 (澳大利亚)
"""

import os
import sys
import logging
import requests
from datetime import datetime, date
from typing import List, Dict, Optional

sys.path.insert(0, "/app")
from database import get_db

logger = logging.getLogger("energypulse.supply_chain.port")


class PortInventoryCollector:
    """港口库存采集器"""
    
    KEY_PORTS = {
        "qinhuangdao": {
            "name": "秦皇岛港",
            "country": "中国",
            "capacity_mt": 200,  # 设计容量(百万吨)
            "warning_level": 500,  # 警戒线(万吨)
            "critical_level": 600,
        },
        "caofeidian": {
            "name": "曹妃甸港",
            "country": "中国",
            "capacity_mt": 300,
            "warning_level": 800,
        },
        "huaibeihua": {
            "name": "黄骅港",
            "country": "中国", 
            "capacity_mt": 200,
            "warning_level": 500,
        },
    }
    
    def __init__(self):
        self.db = get_db()
        
    def collect_qinhuangdao_inventory(self) -> Optional[Dict]:
        """
        采集秦皇岛港库存
        
        数据来源：
        - 秦皇岛煤炭网 (需爬取)
        - 中国煤炭市场网
        -  Wind/同花顺iFinD (API)
        """
        logger.info("采集秦皇岛港库存...")
        
        # 尝试从数据库获取最新数据
        try:
            sql = """
                SELECT value, date, change_pct
                FROM commodity_daily
                WHERE commodity_id = %s
                ORDER BY trade_date DESC LIMIT 1
            """
            # 秦皇岛库存代码
            result = self.db.query(sql, ("COAL_QHD_INV",))
            
            if result:
                return {
                    "port": "qinhuangdao",
                    "inventory_mt": result[0]["value"],  # 万吨
                    "date": result[0]["date"].isoformat(),
                    "change_pct": result[0].get("change_pct", 0),
                    "source": "database"
                }
        except Exception as e:
            logger.warning(f"数据库查询失败: {e}")
        
        return None
    
    def classify_inventory_level(self, port: str, inventory: float) -> str:
        """分类库存水平"""
        config = self.KEY_PORTS.get(port, {})
        warning = config.get("warning_level", 500)
        critical = config.get("critical_level", 600)
        
        if inventory >= critical:
            return "critical_high"  # 极高，压制价格
        elif inventory >= warning:
            return "high"  # 偏高
        elif inventory >= warning * 0.5:
            return "normal"  # 正常
        else:
            return "low"  # 偏低，支撑价格
    
    def get_inventory_analysis(self) -> Dict:
        """获取库存分析"""
        qhd = self.collect_qinhuangdao_inventory()
        
        if not qhd:
            return {
                "status": "no_data",
                "interpretation": "暂无港口库存数据"
            }
        
        level = self.classify_inventory_level("qinhuangdao", qhd["inventory_mt"])
        
        interpretations = {
            "critical_high": "库存极高，港口拥堵，煤炭价格承压",
            "high": "库存偏高，短期价格偏弱",
            "normal": "库存正常，价格由供需平衡决定", 
            "low": "库存偏低，供应紧张支撑价格"
        }
        
        return {
            "status": "ok",
            "qinhuangdao": {
                "inventory": qhd["inventory_mt"],
                "level": level,
                "change_pct": qhd.get("change_pct", 0)
            },
            "interpretation": interpretations.get(level, "未知"),
            "price_implication": "看跌" if level in ["critical_high", "high"] else "看涨" if level == "low" else "中性"
        }
    
    def save_inventory(self, port: str, inventory: float, change_pct: float = 0):
        """保存库存数据（手动更新入口）"""
        try:
            port_codes = {
                "qinhuangdao": "COAL_QHD_INV",
                "caofeidian": "COAL_CFD_INV",
            }
            code = port_codes.get(port, f"COAL_{port.upper()}_INV")
            
            sql = """
                INSERT INTO commodity_daily (commodity_id, trade_date, value, change_pct, source)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (commodity_id, trade_date) DO UPDATE SET
                    value = EXCLUDED.value,
                    change_pct = EXCLUDED.change_pct
            """
            self.db.execute(sql, (code, date.today(), inventory, change_pct, "manual"))
            logger.info(f"{port}库存已保存: {inventory}万吨")
        except Exception as e:
            logger.error(f"保存库存失败: {e}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    collector = PortInventoryCollector()
    
    analysis = collector.get_inventory_analysis()
    print(f"库存分析: {analysis}")
