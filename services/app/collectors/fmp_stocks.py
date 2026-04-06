"""
FMP美股标杆股票+大宗商品+外汇采集器
主通道: Financial Modeling Prep API (stable端点)
备用: Tiingo → Alpha Vantage
"""

import time
import logging
from datetime import date, timedelta

from collectors.base import BaseCollector

logger = logging.getLogger("energypulse.fmp")

# 美股监控池
US_STOCK_SYMBOLS = ["BTU", "CNR", "ARLP", "HCC", "VST", "NRG", "SO"]
US_ETF_SYMBOLS = ["KOL", "XLE"]

# 大宗商品（FMP ticker格式）- 逐个获取
COMMODITY_SYMBOLS = ["CLUSD", "BZUSD", "NGUSD", "GCUSD", "HGUSD"]
COMMODITY_MAP = {
    "CLUSD":  {"id": "WTI",          "unit": "USD/bbl",  "satellite": False},
    "BZUSD":  {"id": "BRENT",        "unit": "USD/bbl",  "satellite": False},
    "NGUSD":  {"id": "HENRY_HUB",    "unit": "USD/MMBtu","satellite": False},
    "GCUSD":  {"id": "COMEX_GOLD",   "unit": "USD/oz",   "satellite": True},
    "HGUSD":  {"id": "LME_COPPER",   "unit": "USD/lb",   "satellite": True},
}

# 外汇+指数
QUOTE_SYMBOLS = ["DX-Y.NYB", "^VIX"]
QUOTE_MAP = {
    "DX-Y.NYB":  {"id": "DXY",       "unit": "index",  "satellite": False},
    "^VIX":      {"id": "VIX",       "unit": "index",  "satellite": False},
}


class FMPUSStockCollector(BaseCollector):
    """FMP美股标杆采集器 - 使用stable端点逐个获取"""

    def collect_primary(self) -> list[dict]:
        records = []
        all_symbols = US_STOCK_SYMBOLS + US_ETF_SYMBOLS

        # 1. 美股个股 - 逐个获取（stable端点不支持批量）
        for symbol in all_symbols:
            try:
                data = self.api_get("fmp", f"/stable/quote?symbol={symbol}")
                if data and isinstance(data, list) and len(data) > 0:
                    item = data[0]
                    records.append({
                        "trade_date": date.today().isoformat(),
                        "market": "US",
                        "symbol": item["symbol"],
                        "name": item.get("name", ""),
                        "open": item.get("open"),
                        "high": item.get("dayHigh"),
                        "low": item.get("dayLow"),
                        "close": item.get("price"),
                        "volume": item.get("volume"),
                        "turnover": None,
                        "change_pct": item.get("changePercentage"),
                        "degraded": False,
                    })
                    logger.debug(f"FMP {symbol} 成功: ${item.get(price)}")
                else:
                    logger.warning(f"FMP {symbol} 无数据")
                time.sleep(0.5)  # 控制频率
            except Exception as e:
                logger.warning(f"FMP {symbol} 失败: {e}")
                time.sleep(1)

        # 2. 大宗商品 - 逐个获取
        for symbol in COMMODITY_SYMBOLS:
            try:
                data = self.api_get("fmp", f"/stable/quote?symbol={symbol}")
                if data and isinstance(data, list) and len(data) > 0:
                    item = data[0]
                    mapping = COMMODITY_MAP.get(symbol, {})
                    if mapping:
                        records.append({
                            "_type": "commodity",
                            "trade_date": date.today().isoformat(),
                            "commodity_id": mapping["id"],
                            "value": item.get("price"),
                            "unit": mapping["unit"],
                            "change_pct": item.get("changePercentage"),
                            "is_satellite": mapping["satellite"],
                            "source": "fmp",
                            "degraded": False,
                        })
                time.sleep(0.5)
            except Exception as e:
                logger.warning(f"FMP commodity {symbol} 失败: {e}")
                time.sleep(1)

        # 3. 外汇+指数 - 逐个获取
        for symbol in QUOTE_SYMBOLS:
            try:
                # 对 ^VIX 等特殊符号进行 URL 编码
                encoded_symbol = symbol.replace("^", "%5E")
                data = self.api_get("fmp", f"/stable/quote?symbol={encoded_symbol}")
                if data and isinstance(data, list) and len(data) > 0:
                    item = data[0]
                    mapping = QUOTE_MAP.get(symbol, {})
                    if mapping:
                        records.append({
                            "_type": "commodity",
                            "trade_date": date.today().isoformat(),
                            "commodity_id": mapping["id"],
                            "value": item.get("price"),
                            "unit": mapping["unit"],
                            "change_pct": item.get("changePercentage"),
                            "is_satellite": mapping["satellite"],
                            "source": "fmp",
                            "degraded": False,
                        })
                time.sleep(0.5)
            except Exception as e:
                logger.warning(f"FMP index {symbol} 失败: {e}")

        logger.info(f"FMP 采集完成，共 {len(records)} 条记录")
        return records

    def collect_fallback(self) -> list[dict]:
        """备用通道: Tiingo"""
        records = []
        for symbol in US_STOCK_SYMBOLS + US_ETF_SYMBOLS:
            try:
                data = self.api_get("tiingo", f"/tiingo/daily/{symbol}/prices",
                                    params={"startDate": date.today().isoformat()})
                if data:
                    item = data[0] if isinstance(data, list) else data
                    records.append({
                        "trade_date": date.today().isoformat(),
                        "market": "US",
                        "symbol": symbol,
                        "name": "",
                        "open": item.get("open"),
                        "high": item.get("high"),
                        "low": item.get("low"),
                        "close": item.get("close"),
                        "volume": item.get("volume"),
                        "turnover": None,
                        "change_pct": None,
                        "degraded": False,
                    })
                    logger.info(f"Tiingo {symbol} 成功")
                time.sleep(2)
            except Exception as e:
                logger.warning(f"Tiingo {symbol} 失败: {e}")
        return records

    def validate(self, records: list[dict]) -> list[dict]:
        valid = []
        for r in records:
            if r.get("_type") == "commodity":
                if r.get("value") is not None and r["value"] > 0:
                    valid.append(r)
            else:
                if r.get("close") is not None and r["close"] > 0:
                    valid.append(r)
        return valid

    def store(self, records: list[dict]):
        stocks = [r for r in records if r.get("_type") != "commodity"]
        commodities = [r for r in records if r.get("_type") == "commodity"]

        # 清理_type标记
        for c in commodities:
            c.pop("_type", None)

        if stocks:
            self.db.upsert_stock(stocks)
            logger.info(f"美股行情写入 {len(stocks)} 条")

        if commodities:
            self.db.upsert_commodity(commodities)
            logger.info(f"商品数据写入 {len(commodities)} 条")
