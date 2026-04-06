"""
FRED宏观经济数据采集器 v3
添加美元指数、欧洲PMI、中国PMI
"""

import logging
import requests
from collectors.base import BaseCollector

logger = logging.getLogger("energypulse.fred")

FRED_API_KEY = "451d2a25c4a4db537f1168cb27d28223"

# 完整的FRED指标库
FRED_SERIES = {
    # ===== 美国核心宏观 =====
    "FEDFUNDS": {"name": "联邦基金利率", "unit": "%", "period": "monthly", "region": "美国"},
    "DGS10": {"name": "美国10Y国债收益率", "unit": "%", "period": "daily", "region": "美国"},
    "DGS2": {"name": "美国2Y国债收益率", "unit": "%", "period": "daily", "region": "美国"},
    
    # ===== 美元指数 (新增) =====
    "DTWEXBGS": {"name": "美元指数(DXY)", "unit": "指数", "period": "daily", "region": "全球"},
    
    # ===== 通胀 =====
    "CPIAUCSL": {"name": "CPI消费者物价", "unit": "指数", "period": "monthly", "region": "美国"},
    "PPIACO": {"name": "PPI生产者价格指数", "unit": "指数", "period": "monthly", "region": "美国"},
    "CPILFESL": {"name": "核心CPI", "unit": "指数", "period": "monthly", "region": "美国"},
    
    # ===== 就业 =====
    "PAYEMS": {"name": "非农就业人数", "unit": "千人", "period": "monthly", "region": "美国"},
    "UNRATE": {"name": "失业率", "unit": "%", "period": "monthly", "region": "美国"},
    "ICSA": {"name": "首次申请失业救济", "unit": "人", "period": "weekly", "region": "美国"},
    
    # ===== 工业 =====
    "INDPRO": {"name": "工业产出指数", "unit": "指数", "period": "monthly", "region": "美国"},
    "TCU": {"name": "产能利用率", "unit": "%", "period": "monthly", "region": "美国"},
    
    # ===== 欧洲宏观 (新增) =====
    "EA19MARMFGPMI": {"name": "欧元区制造业PMI", "unit": "指数", "period": "monthly", "region": "欧洲"},
    "EA19MARSVPMI": {"name": "欧元区服务业PMI", "unit": "指数", "period": "monthly", "region": "欧洲"},
    "CP0000EZ19M086NEST": {"name": "欧元区CPI", "unit": "%", "period": "monthly", "region": "欧洲"},
    
    # ===== 中国宏观 (新增) =====
    "CHNPMI": {"name": "中国制造业PMI", "unit": "指数", "period": "monthly", "region": "中国"},
    
    # ===== 其他 =====
    "GDP": {"name": "美国GDP", "unit": "十亿美元", "period": "quarterly", "region": "美国"},
    "BOPGSTB": {"name": "美国贸易差额", "unit": "十亿美元", "period": "monthly", "region": "美国"},
}


class FREDMacroCollector(BaseCollector):
    """FRED宏观数据采集器"""

    def collect_primary(self) -> list[dict]:
        records = []
        
        logger.info(f"开始采集FRED宏观数据，共{len(FRED_SERIES)}个指标")
        
        for series_id, meta in FRED_SERIES.items():
            try:
                url = "https://api.stlouisfed.org/fred/series/observations"
                params = {
                    "series_id": series_id,
                    "api_key": FRED_API_KEY,
                    "file_type": "json",
                    "limit": 1,
                    "sort_order": "desc",
                }
                
                resp = requests.get(url, params=params, timeout=30)
                resp.raise_for_status()
                data = resp.json()
                
                observations = data.get("observations", [])
                
                if not observations:
                    logger.warning(f"{series_id} 无数据")
                    continue
                
                obs = observations[0]
                if obs.get("value") in [".", None, ""]:
                    continue
                
                record = {
                    "time": obs["date"] + "T00:00:00Z",
                    "source_id": "FRED",
                    "series_id": series_id,
                    "series_name": meta["name"],
                    "value": float(obs["value"]),
                    "unit": meta["unit"],
                    "period_type": meta["period"],
                    "region": meta.get("region", "其他"),
                    "raw_url": "https://fred.stlouisfed.org/series/" + series_id,
                    "raw_hash": self.make_hash(obs),
                    "verified": True,
                    "degraded": False,
                }
                records.append(record)
                
                series_name = meta["name"]
                value = obs["value"]
                unit = meta["unit"]
                region = meta.get("region", "")
                logger.info(f"[{region}] {series_name}: {value} {unit}")
                    
            except Exception as e:
                logger.error(f"获取{series_id}失败: {e}")
        
        logger.info(f"FRED采集完成: {len(records)}条")
        return records
    
    def store(self, records: list[dict]):
        if records:
            self.db.upsert_macro(records)
            logger.info(f"FRED宏观数据写入 {len(records)} 条")


if __name__ == "__main__":
    from database import get_db
    db = get_db()
    collector = FREDMacroCollector("fred_macro")
    records = collector.collect_primary()
    print(f"\n采集完成: {len(records)} 条")
    for r in records:
        region = r.get("region", "")
        print(f"  [{region}] {r["series_name"]}: {r["value"]} {r["unit"]}")
