import requests
import logging
from datetime import date
from database import get_db

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("energypulse.eia")

EIA_API_KEY = "VASndGxaiSNjvHaEUelCiN20fJIiNbB9r0HVZEZM"
EIA_BASE = "https://api.eia.gov/v2"

# 能源价格 + 库存数据
SERIES = {
    # 价格数据 (日度)
    "PET.RWTC.D": {"name": "WTI原油", "unit": "美元/桶", "cat": "OIL", "type": "price"},
    "NG.RNGWHHD.D": {"name": "Henry Hub天然气", "unit": "美元/MMBtu", "cat": "GAS", "type": "price"},
    
    # 库存数据 (周度) - 关键供需指标
    "PET.WCESTUS1.W": {"name": "美国商业原油库存", "unit": "千桶", "cat": "OIL_INV", "type": "inventory", "freq": "weekly"},
    "NG.NW2_EPG0_SWO_R48_BCF.W": {"name": "美国天然气库存", "unit": "十亿立方英尺", "cat": "GAS_INV", "type": "inventory", "freq": "weekly"},
}

def fetch_eia_series(series_id, freq="daily"):
    """获取EIA数据系列"""
    url = f"{EIA_BASE}/seriesid/{series_id}/"
    params = {
        "api_key": EIA_API_KEY,
        "frequency": freq,
        "data[0]": "value",
        "sort[0][column]": "period",
        "sort[0][direction]": "desc",
        "length": 2,
    }
    
    try:
        r = requests.get(url, params=params, timeout=30)
        data = r.json()
        return data.get("response", {}).get("data", [])
    except Exception as e:
        logger.error(f"EIA请求失败 {series_id}: {e}")
        return []

def collect_eia():
    db = get_db()
    today = date.today().isoformat()
    records = []
    
    for sid, meta in SERIES.items():
        try:
            freq = meta.get("freq", "daily")
            items = fetch_eia_series(sid, freq)
            
            if len(items) >= 1:
                val = float(items[0]["value"])
                chg = 0
                if len(items) >= 2:
                    prev = float(items[1]["value"])
                    chg = round(((val - prev) / prev) * 100, 2)
                
                # 库存数据变化说明
                if meta.get("type") == "inventory":
                    # 库存增加 = 供给过剩 = 利空
                    # 库存减少 = 需求强劲 = 利多
                    signal = "利空" if chg > 0 else "利多" if chg < 0 else "中性"
                    logger.info(f"{meta["name"]}: {val} {meta["unit"]} (变化{chg}%, {signal})")
                else:
                    logger.info(f"{meta["name"]}: ${val} ({chg}%)")
                
                rec = {
                    "commodity_id": meta["cat"],
                    "trade_date": items[0].get("period", today),
                    "value": round(val, 2),
                    "change_pct": chg,
                    "source": "EIA",
                }
                records.append(rec)
        except Exception as e:
            logger.error(f"{meta["name"]} failed: {e}")
    
    # 存储到数据库
    for r in records:
        sql = """
            INSERT INTO commodity_daily (commodity_id, trade_date, value, change_pct, source)
            VALUES (%(commodity_id)s, %(trade_date)s, %(value)s, %(change_pct)s, %(source)s)
            ON CONFLICT (commodity_id, trade_date) DO UPDATE SET
                value = EXCLUDED.value, change_pct = EXCLUDED.change_pct
        """
        db.execute(sql, r)
    
    return records

if __name__ == "__main__":
    records = collect_eia()
    print(f"EIA采集完成: {len(records)} 条")
    for r in records:
        cid = r["commodity_id"]
        val = r["value"]
        chg = r["change_pct"]
        print(f"  {cid}: {val} ({chg}%)")
