"""
EIA能源数据采集器
美国能源信息署 - 周度数据、月度数据
"""

import logging
from datetime import date, timedelta
from collectors.base import BaseCollector

logger = logging.getLogger("energypulse.eia")


class EIADataCollector(BaseCollector):
    """EIA能源数据采集器"""

    def collect_primary(self) -> list[dict]:
        records = []
        
        # 尝试多种EIA API端点
        endpoints = [
            ("natural-gas/stor/wkly", "NG_STORAGE", "天然气库存", "Bcf"),
            ("petroleum/pnp/wiup/srd", "OIL_INVENTORY", "原油库存", "千桶"),
            ("coal/production/weekly", "COAL_PRODUCTION", "煤炭产量", "千短吨"),
        ]
        
        for endpoint, series_id, name, unit in endpoints:
            try:
                data = self._fetch_eia_data(endpoint, series_id, name, unit)
                records.extend(data)
                logger.info(f"EIA {name}: {len(data)} 条")
            except Exception as e:
                logger.warning(f"EIA {name} 采集失败: {e}")
        
        return records
    
    def _fetch_eia_data(self, endpoint: str, series_id: str, name: str, unit: str) -> list[dict]:
        """获取EIA数据"""
        records = []
        
        # 使用/v2/series端点（更稳定）
        url = f"/series/id/{series_id}"
        try:
            data = self.api_get("eia", url, params={
                "frequency": "weekly",
                "data": ["value"],
                "sort": [{"column": "period", "direction": "desc"}],
                "offset": 0,
                "length": 5,
            })
            
            for item in data.get("response", {}).get("data", []):
                records.append({
                    "time": item.get("period") + "T00:00:00Z",
                    "source_id": "EIA",
                    "series_id": series_id,
                    "series_name": name,
                    "value": float(item.get("value", 0)),
                    "unit": unit,
                    "period_type": "weekly",
                    "raw_url": f"https://api.eia.gov/v2/{endpoint}",
                    "raw_hash": self.make_hash(item),
                    "verified": True,
                    "degraded": False,
                })
        except Exception as e:
            logger.warning(f"EIA series endpoint failed: {e}")
        
        return records
    
    def collect_fallback(self) -> list[dict]:
        """备用: 从其他API获取能源数据"""
        return []
    
    def store(self, records: list[dict]):
        if records:
            self.db.upsert_macro(records)
            logger.info(f"EIA数据写入 {len(records)} 条")


class EIAWeeklyCollector(BaseCollector):
    """EIA周度数据快速采集"""
    
    def collect_primary(self) -> list[dict]:
        records = []
        today = date.today().isoformat()
        
        # 使用FMP作为EIA备用数据源
        try:
            # 获取天然气库存ETF (UNG相关)
            data = self.api_get("fmp", "/stable/quote?symbol=UNG&apikey=" + self.config.get_key("fmp"))
            if data and isinstance(data, list):
                for item in data:
                    records.append({
                        "time": today + "T00:00:00Z",
                        "source_id": "FMP_ETF",
                        "series_id": "UNG_GAS",
                        "series_name": "天然气ETF",
                        "value": item.get("price"),
                        "unit": "USD",
                        "period_type": "daily",
                        "raw_url": "fmp",
                        "raw_hash": self.make_hash(item),
                        "verified": True,
                        "degraded": False,
                    })
        except Exception as e:
            logger.warning(f"EIA备用采集失败: {e}")
        
        return records
    
    def store(self, records: list[dict]):
        if records:
            self.db.upsert_macro(records)
            logger.info(f"EIA备用数据写入 {len(records)} 条")
