"""
电厂日耗煤采集器

数据来源：
- 中国电力企业联合会 (CEC)
- 六大发电集团数据
- 沿海电厂数据
"""

import os
import sys
import logging
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional

sys.path.insert(0, "/app")
from database import get_db

logger = logging.getLogger("energypulse.supply_chain.power")


class PowerConsumptionCollector:
    """电厂日耗采集器"""
    
    # 六大发电集团
    POWER_GROUPS = [
        "华能", "大唐", "华电", "国电", "国投电力", "华润电力"
    ]
    
    # 季节参考值 (万吨/日)
    SEASONAL_BENCHMARK = {
        "summer_peak": 80,    # 夏季高峰
        "winter_peak": 75,    # 冬季高峰
        "shoulder": 60,       # 淡季
        "normal": 65,         # 正常
    }
    
    def __init__(self):
        self.db = get_db()
        
    def collect_daily_consumption(self) -> Optional[Dict]:
        """
        采集六大电日耗煤数据
        
        数据来源：
        - 中国煤炭资源网
        - 中国电力企业联合会日报
        - CCTD中国煤炭市场网
        """
        logger.info("采集电厂日耗...")
        
        try:
            sql = """
                SELECT value, date
                FROM commodity_daily
                WHERE commodity_id = %s
                ORDER BY trade_date DESC LIMIT 1
            """
            result = self.db.query(sql, ("COAL_POWER_DAILY",))
            
            if result:
                return {
                    "daily_consumption": result[0]["value"],  # 万吨
                    "date": result[0]["date"].isoformat(),
                    "coverage": "六大发电集团"
                }
        except Exception as e:
            logger.warning(f"数据库查询失败: {e}")
        
        return None
    
    def collect_inventory_days(self) -> Optional[Dict]:
        """采集存煤可用天数"""
        logger.info("采集电厂存煤天数...")
        
        try:
            sql = """
                SELECT value, date
                FROM commodity_daily
                WHERE commodity_id = %s
                ORDER BY trade_date DESC LIMIT 1
            """
            result = self.db.query(sql, ("COAL_POWER_DAYS",))
            
            if result:
                return {
                    "inventory_days": result[0]["value"],
                    "date": result[0]["date"].isoformat(),
                    "warning_level": 15,  # 警戒线15天
                    "critical_level": 7   # 危险线7天
                }
        except Exception as e:
            logger.warning(f"数据库查询失败: {e}")
        
        return None
    
    def analyze_power_demand(self) -> Dict:
        """分析电力煤炭需求"""
        consumption = self.collect_daily_consumption()
        inventory_days = self.collect_inventory_days()
        
        if not consumption:
            return {
                "status": "no_data",
                "interpretation": "暂无电厂日耗数据"
            }
        
        daily = consumption["daily_consumption"]
        
        # 判断季节水平
        month = date.today().month
        if month in [7, 8]:
            season = "summer_peak"
        elif month in [12, 1]:
            season = "winter_peak"
        elif month in [4, 5, 9, 10]:
            season = "shoulder"
        else:
            season = "normal"
        
        benchmark = self.SEASONAL_BENCHMARK.get(season, 65)
        
        # 需求强度
        if daily > benchmark * 1.1:
            demand_strength = "强劲"
            demand_implication = "日耗高于季节均值，煤炭需求旺盛"
        elif daily > benchmark * 0.9:
            demand_strength = "正常"
            demand_implication = "日耗符合季节规律"
        else:
            demand_strength = "偏弱"
            demand_implication = "日耗低于季节均值，需求疲软"
        
        # 库存分析
        inv_analysis = ""
        if inventory_days:
            days = inventory_days["inventory_days"]
            if days < 7:
                inv_analysis = f"存煤仅{days}天，远低于安全线，急需补库"
            elif days < 15:
                inv_analysis = f"存煤{days}天，低于警戒线，补库需求上升"
            else:
                inv_analysis = f"存煤{days}天，库存充足"
        
        return {
            "status": "ok",
            "daily_consumption": daily,
            "season": season,
            "benchmark": benchmark,
            "demand_strength": demand_strength,
            "demand_implication": demand_implication,
            "inventory_analysis": inv_analysis,
            "price_implication": "看涨" if demand_strength == "强劲" and inventory_days and inventory_days["inventory_days"] < 15 else "中性"
        }
    
    def save_consumption(self, daily_mt: float, inventory_days: Optional[float] = None):
        """保存日耗数据（手动更新入口）"""
        try:
            today = date.today()
            
            # 日耗
            sql1 = """
                INSERT INTO commodity_daily (commodity_id, trade_date, value, source)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (commodity_id, trade_date) DO UPDATE SET
                    value = EXCLUDED.value
            """
            self.db.execute(sql1, ("COAL_POWER_DAILY", today, daily_mt, "manual"))
            
            # 存煤天数
            if inventory_days:
                sql2 = """
                    INSERT INTO commodity_daily (commodity_id, trade_date, value, source)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (commodity_id, trade_date) DO UPDATE SET
                        value = EXCLUDED.value
                """
                self.db.execute(sql2, ("COAL_POWER_DAYS", today, inventory_days, "manual"))
            
            logger.info(f"电厂数据已保存: 日耗{daily_mt}万吨, 存煤{inventory_days}天")
            
        except Exception as e:
            logger.error(f"保存电厂数据失败: {e}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    collector = PowerConsumptionCollector()
    
    analysis = collector.analyze_power_demand()
    print(f"电力需求分析: {analysis}")
