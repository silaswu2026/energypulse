"""
煤炭产业链数据细分采集器

覆盖产业链各环节：
1. 上游: 煤炭开采、洗选
2. 中游: 物流运输、港口库存
3. 下游: 电力、钢铁、化工需求
"""

import os
import sys
import logging
from datetime import datetime, date
from typing import Dict, List, Optional
from dataclasses import dataclass

sys.path.insert(0, "/app")
from database import get_db

logger = logging.getLogger("energypulse.coal_chain")


@dataclass
class CoalPrice:
    """煤炭价格数据"""
    coal_type: str  # 动力煤/焦煤/无烟煤
    benchmark: str  # 秦皇岛5500K/吕梁主焦煤等
    price: float
    change_pct: float
    date: date
    source: str


class CoalChainCollector:
    """煤炭产业链采集器"""
    
    # 煤炭品种定义
    COAL_TYPES = {
        "thermal": {
            "name": "动力煤",
            "benchmarks": {
                "qhd_5500": {"name": "秦皇岛港5500K", "unit": "元/吨"},
                "ces_5500": {"name": "环渤海5500K", "unit": "元/吨"},
                "cci_5500": {"name": "CCI 5500", "unit": "元/吨"},
            },
            "downstream": ["电力", "供暖"],
        },
        "coking": {
            "name": "焦煤",
            "benchmarks": {
                "lvliang": {"name": "吕梁主焦煤", "unit": "元/吨"},
                "xishan": {"name": "西山主焦煤", "unit": "元/吨"},
                "aus_plv": {"name": "澳洲PLV", "unit": "美元/吨"},
            },
            "downstream": ["焦炭", "钢铁"],
        },
        "anthracite": {
            "name": "无烟煤",
            "benchmarks": {
                "jincheng": {"name": "晋城无烟煤", "unit": "元/吨"},
            },
            "downstream": ["尿素", "甲醇", "合成氨"],
        },
    }
    
    def __init__(self):
        self.db = get_db()
    
    def get_thermal_coal_prices(self) -> List[CoalPrice]:
        """获取动力煤价格"""
        prices = []
        
        # 从数据库获取各基准价格
        benchmarks = ["QHD_5500", "CES_5500", "CCI_5500"]
        
        for benchmark in benchmarks:
            try:
                sql = """
                    SELECT value, change_pct, trade_date
                    FROM commodity_daily
                    WHERE commodity_id = %s
                    ORDER BY trade_date DESC LIMIT 1
                """
                result = self.db.query(sql, (benchmark,))
                
                if result:
                    prices.append(CoalPrice(
                        coal_type="动力煤",
                        benchmark=self.COAL_TYPES["thermal"]["benchmarks"].get(
                            benchmark.lower(), {}).get("name", benchmark),
                        price=result[0]["value"],
                        change_pct=result[0].get("change_pct", 0) or 0,
                        date=result[0]["trade_date"],
                        source="database"
                    ))
            except Exception as e:
                logger.error(f"获取{benchmark}价格失败: {e}")
        
        return prices
    
    def get_coking_coal_prices(self) -> List[CoalPrice]:
        """获取焦煤价格"""
        prices = []
        benchmarks = ["LVLIANG_JM", "XISHAN_JM", "AUS_PLV"]
        
        for benchmark in benchmarks:
            try:
                sql = """
                    SELECT value, change_pct, trade_date
                    FROM commodity_daily
                    WHERE commodity_id = %s
                    ORDER BY trade_date DESC LIMIT 1
                """
                result = self.db.query(sql, (benchmark,))
                
                if result:
                    prices.append(CoalPrice(
                        coal_type="焦煤",
                        benchmark=benchmark,
                        price=result[0]["value"],
                        change_pct=result[0].get("change_pct", 0) or 0,
                        date=result[0]["trade_date"],
                        source="database"
                    ))
            except Exception as e:
                logger.error(f"获取{benchmark}价格失败: {e}")
        
        return prices
    
    def get_downstream_demand(self) -> Dict:
        """获取下游需求指标"""
        demand = {}
        
        try:
            # 电厂日耗（动力煤需求）
            sql = """
                SELECT value, trade_date
                FROM commodity_daily
                WHERE commodity_id = 'COAL_POWER_DAILY'
                ORDER BY trade_date DESC LIMIT 1
            """
            result = self.db.query(sql)
            if result:
                demand["power_plant"] = {
                    "daily_consumption": result[0]["value"],
                    "unit": "万吨/日",
                    "trend": self._classify_demand(result[0]["value"], "power")
                }
            
            # 焦炭价格（焦煤需求指标）
            sql2 = """
                SELECT value, change_pct
                FROM commodity_daily
                WHERE commodity_id = 'COKE_PRICE'
                ORDER BY trade_date DESC LIMIT 1
            """
            result2 = self.db.query(sql2)
            if result2:
                demand["coke"] = {
                    "price": result2[0]["value"],
                    "change_pct": result2[0].get("change_pct", 0),
                    "trend": "up" if (result2[0].get("change_pct", 0) or 0) > 0 else "down"
                }
        
        except Exception as e:
            logger.error(f"获取下游需求失败: {e}")
        
        return demand
    
    def _classify_demand(self, value: float, sector: str) -> str:
        """分类需求水平"""
        if sector == "power":
            if value > 70:
                return "high"
            elif value > 55:
                return "normal"
            else:
                return "low"
        return "unknown"
    
    def get_inventory_status(self) -> Dict:
        """获取产业链库存状态"""
        inventory = {}
        
        try:
            # 港口库存
            sql = """
                SELECT commodity_id, value, trade_date
                FROM commodity_daily
                WHERE commodity_id LIKE 'COAL_%_INV'
                AND trade_date >= CURRENT_DATE - INTERVAL '7 days'
                ORDER BY trade_date DESC
            """
            results = self.db.query(sql)
            
            for row in results:
                port_code = row["commodity_id"]
                if "QHD" in port_code:
                    inventory["qinhuangdao"] = {
                        "volume": row["value"],
                        "unit": "万吨",
                        "status": self._classify_port_inventory(row["value"])
                    }
                elif "CFD" in port_code:
                    inventory["caofeidian"] = {
                        "volume": row["value"],
                        "unit": "万吨",
                        "status": self._classify_port_inventory(row["value"])
                    }
            
            # 电厂库存天数
            sql2 = """
                SELECT value
                FROM commodity_daily
                WHERE commodity_id = 'COAL_POWER_DAYS'
                ORDER BY trade_date DESC LIMIT 1
            """
            result2 = self.db.query(sql2)
            if result2:
                days = result2[0]["value"]
                inventory["power_plant_days"] = {
                    "days": days,
                    "status": "critical" if days < 7 else "warning" if days < 15 else "normal"
                }
        
        except Exception as e:
            logger.error(f"获取库存状态失败: {e}")
        
        return inventory
    
    def _classify_port_inventory(self, volume: float) -> str:
        """分类港口库存水平"""
        if volume > 600:
            return "high"
        elif volume > 400:
            return "normal"
        else:
            return "low"
    
    def generate_chain_report(self) -> Dict:
        """生成产业链报告"""
        thermal_prices = self.get_thermal_coal_prices()
        coking_prices = self.get_coking_coal_prices()
        demand = self.get_downstream_demand()
        inventory = self.get_inventory_status()
        
        return {
            "thermal_coal": {
                "prices": [p.__dict__ for p in thermal_prices],
                "demand": demand.get("power_plant", {}),
            },
            "coking_coal": {
                "prices": [p.__dict__ for p in coking_prices],
                "demand": demand.get("coke", {}),
            },
            "inventory": inventory,
            "analysis": self._generate_analysis(thermal_prices, inventory, demand),
            "timestamp": datetime.utcnow().isoformat()
        }
    
    def _generate_analysis(self, thermal_prices, inventory, demand) -> str:
        """生成分析结论"""
        analyses = []
        
        # 价格分析
        if thermal_prices:
            avg_change = sum(p.change_pct for p in thermal_prices) / len(thermal_prices)
            if avg_change > 2:
                analyses.append(f"动力煤价格整体上涨{avg_change:.1f}%")
            elif avg_change < -2:
                analyses.append(f"动力煤价格整体下跌{abs(avg_change):.1f}%")
        
        # 库存分析
        if inventory.get("qinhuangdao", {}).get("status") == "high":
            analyses.append("秦港库存偏高，压制价格上涨")
        elif inventory.get("qinhuangdao", {}).get("status") == "low":
            analyses.append("秦港库存偏低，支撑价格")
        
        # 需求分析
        if demand.get("power_plant", {}).get("trend") == "high":
            analyses.append("电厂日耗高位，需求旺盛")
        
        return "；".join(analyses) if analyses else "产业链运行平稳"


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    collector = CoalChainCollector()
    report = collector.generate_chain_report()
    print(json.dumps(report, indent=2, ensure_ascii=False, default=str))
