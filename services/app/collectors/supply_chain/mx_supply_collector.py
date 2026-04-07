"""
供应链数据采集器 - 基于妙想(mx-search)

采集: BDI、港口库存、电厂日耗
特点: 使用自然语言查询，正则提取数值
"""

import os
import sys
import re
import logging
from datetime import datetime, date
from typing import Dict, Optional, Tuple

sys.path.insert(0, "/app")
sys.path.insert(0, "/app/skills/mx-search")

from database import get_db
from mx_search import MXSearch

logger = logging.getLogger("energypulse.mx_supply")


class MXSupplyChainCollector:
    """基于妙想的供应链数据采集器"""
    
    def __init__(self):
        self.db = get_db()
        try:
            self.client = MXSearch()
            logger.info("MXSupplyChainCollector initialized")
        except Exception as e:
            logger.error(f"Failed to initialize MXSearch: {e}")
            self.client = None
    
    def collect_bdi(self) -> Optional[Dict]:
        """
        采集BDI指数
        """
        if not self.client:
            return None
        
        try:
            queries = [
                "BDI波罗的海干散货指数最新数值",
                "BDI指数今日数据",
                "波罗的海干散货指数最新"
            ]
            
            for query in queries:
                result = self.client.search(query)
                if result.get("status") != 0:
                    continue
                
                content = MXSearch.extract_content(result)
                if not content:
                    continue
                
                # 提取BDI数值
                matches = re.findall(r'BDI[^0-9]*(\d{3,4})[^0-9]', content, re.IGNORECASE)
                
                if not matches:
                    matches = re.findall(r'指数[^0-9]*(\d{3,4})[^0-9]', content)
                
                if matches:
                    bdi_value = int(matches[0])
                    
                    return {
                        "commodity_id": "BDI",
                        "trade_date": date.today().isoformat(),
                        "value": bdi_value,
                        "change_pct": 0,
                        "unit": "点",
                        "source": "mx-search",
                        "raw_query": query
                    }
            
            logger.warning("BDI data extraction failed")
            return None
            
        except Exception as e:
            logger.error(f"采集BDI失败: {e}")
            return None
    
    def collect_port_inventory(self, port: str = "qinhuangdao") -> Optional[Dict]:
        """
        采集港口库存
        """
        if not self.client:
            return None
        
        port_queries = {
            "qinhuangdao": "秦皇岛港煤炭库存",
            "caofeidian": "曹妃甸港煤炭库存",
            "huanghua": "黄骅港煤炭库存"
        }
        
        query_base = port_queries.get(port, "秦皇岛港煤炭库存")
        
        try:
            queries = [
                f"{query_base}最新数据",
                f"{query_base}今日",
                f"{query_base}万吨"
            ]
            
            for query in queries:
                result = self.client.search(query)
                if result.get("status") != 0:
                    continue
                
                content = MXSearch.extract_content(result)
                if not content:
                    continue
                
                # 提取库存数值
                matches = re.findall(r'库存[^0-9]*(\d{2,4})[^0-9]*万吨', content)
                if not matches:
                    matches = re.findall(r'(\d{3})[^0-9]*万吨', content)
                
                if matches:
                    inventory = float(matches[0])
                    
                    return {
                        "commodity_id": f"COAL_{port.upper()}_INV",
                        "trade_date": date.today().isoformat(),
                        "value": inventory,
                        "change_pct": 0,
                        "unit": "万吨",
                        "source": "mx-search",
                        "raw_query": query
                    }
            
            logger.warning(f"{port} inventory extraction failed")
            return None
            
        except Exception as e:
            logger.error(f"采集{port}库存失败: {e}")
            return None
    
    def collect_power_consumption(self) -> Optional[Dict]:
        """
        采集电厂日耗煤
        """
        if not self.client:
            return None
        
        try:
            queries = [
                "六大发电集团煤炭日耗量最新",
                "六大电厂日耗煤数据",
                "重点电厂煤炭日耗"
            ]
            
            for query in queries:
                result = self.client.search(query)
                if result.get("status") != 0:
                    continue
                
                content = MXSearch.extract_content(result)
                if not content:
                    continue
                
                # 提取日耗数值
                matches = re.findall(r'日耗[^0-9]*(\d{1,2}\.?\d{0,2})[^0-9]*万吨', content)
                if not matches:
                    matches = re.findall(r'(\d{2})[^0-9]*万吨', content)
                
                if matches:
                    consumption = float(matches[0])
                    
                    return {
                        "commodity_id": "COAL_POWER_DAILY",
                        "trade_date": date.today().isoformat(),
                        "value": consumption,
                        "change_pct": 0,
                        "unit": "万吨/日",
                        "source": "mx-search",
                        "raw_query": query
                    }
            
            logger.warning("Power consumption extraction failed")
            return None
            
        except Exception as e:
            logger.error(f"采集电厂日耗失败: {e}")
            return None
    
    def save_to_database(self, data: Dict) -> bool:
        """保存数据到数据库"""
        if not data:
            return False
        
        try:
            sql = """
                INSERT INTO commodity_daily 
                (commodity_id, trade_date, value, change_pct, unit, source)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (commodity_id, trade_date) DO UPDATE SET
                    value = EXCLUDED.value,
                    change_pct = EXCLUDED.change_pct,
                    source = EXCLUDED.source
            """
            
            self.db.execute(sql, (
                data["commodity_id"],
                data["trade_date"],
                data["value"],
                data.get("change_pct", 0),
                data.get("unit", ""),
                data.get("source", "mx-search")
            ))
            
            logger.info(f"已保存 {data['commodity_id']}: {data['value']} {data.get('unit', '')}")
            return True
            
        except Exception as e:
            logger.error(f"保存数据失败: {e}")
            return False
    
    def collect_all(self) -> Dict[str, bool]:
        """采集所有供应链数据"""
        results = {}
        
        # 采集BDI
        bdi_data = self.collect_bdi()
        results["BDI"] = self.save_to_database(bdi_data) if bdi_data else False
        
        # 采集港口库存
        qhd_data = self.collect_port_inventory("qinhuangdao")
        results["QHD_Inventory"] = self.save_to_database(qhd_data) if qhd_data else False
        
        # 采集电厂日耗
        power_data = self.collect_power_consumption()
        results["Power_Daily"] = self.save_to_database(power_data) if power_data else False
        
        return results


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    collector = MXSupplyChainCollector()
    results = collector.collect_all()
    
    print("\n采集结果:")
    for key, success in results.items():
        status = "✅" if success else "❌"
        print(f"  {status} {key}")
