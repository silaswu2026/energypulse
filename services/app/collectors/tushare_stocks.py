"""
Tushare A股目标股票采集器
行情数据 + 北向资金
主通道: Tushare Pro API
备用: AKShare
"""

import logging
from datetime import date, timedelta

from collectors.base import BaseCollector

logger = logging.getLogger("energypulse.tushare")

CN_STOCK_SYMBOLS = [
    ("601088.SH", "中国神华"),
    ("601225.SH", "陕西煤业"),
    ("601898.SH", "中煤能源"),
    ("600900.SH", "长江电力"),
    ("600011.SH", "华能国际"),
    ("600027.SH", "华电国际"),
    ("600188.SH", "兖矿能源"),
]


class TushareCNStockCollector(BaseCollector):
    """A股目标股票采集器"""

    def __init__(self, source_id: str):
        super().__init__(source_id)
        self._ts_api = None

    @property
    def ts_api(self):
        if self._ts_api is None:
            import tushare as ts
            token = self.config.get_key("tushare")
            ts.set_token(token)
            self._ts_api = ts.pro_api()
        return self._ts_api

    def collect_primary(self) -> list[dict]:
        records = []
        trade_date = date.today().strftime("%Y%m%d")

        # 1. 日线行情
        symbols_str = ",".join([s[0] for s in CN_STOCK_SYMBOLS])
        try:
            df = self.ts_api.daily(
                ts_code=symbols_str,
                start_date=(date.today() - timedelta(days=5)).strftime("%Y%m%d"),
                end_date=trade_date,
            )
            if df is not None and not df.empty:
                name_map = {s[0]: s[1] for s in CN_STOCK_SYMBOLS}
                for _, row in df.iterrows():
                    records.append({
                        "trade_date": f"{row['trade_date'][:4]}-{row['trade_date'][4:6]}-{row['trade_date'][6:]}",
                        "market": "CN",
                        "symbol": row["ts_code"],
                        "name": name_map.get(row["ts_code"], ""),
                        "open": row.get("open"),
                        "high": row.get("high"),
                        "low": row.get("low"),
                        "close": row.get("close"),
                        "volume": int(row.get("vol", 0) * 100) if row.get("vol") else None,
                        "turnover": row.get("amount") * 1000 if row.get("amount") else None,
                        "change_pct": row.get("pct_chg"),
                        "degraded": False,
                    })
        except Exception as e:
            logger.error(f"Tushare日线采集失败: {e}")
            raise

        # 2. 北向资金
        try:
            hsgt = self.ts_api.moneyflow_hsgt(
                start_date=(date.today() - timedelta(days=3)).strftime("%Y%m%d"),
                end_date=trade_date,
            )
            if hsgt is not None and not hsgt.empty:
                for _, row in hsgt.iterrows():
                    td = f"{row['trade_date'][:4]}-{row['trade_date'][4:6]}-{row['trade_date'][6:]}"
                    records.append({
                        "_type": "northbound",
                        "trade_date": td,
                        "north_net": row.get("north_money"),
                    })
        except Exception as e:
            logger.warning(f"北向资金采集失败: {e}")

        return records

    def collect_fallback(self) -> list[dict]:
        """备用: AKShare"""
        records = []
        try:
            import akshare as ak
            for symbol, name in CN_STOCK_SYMBOLS:
                code = symbol.split(".")[0]
                df = ak.stock_zh_a_hist(
                    symbol=code, period="daily",
                    start_date=(date.today() - timedelta(days=5)).strftime("%Y%m%d"),
                    end_date=date.today().strftime("%Y%m%d"),
                    adjust="qfq"
                )
                if df is not None and not df.empty:
                    for _, row in df.iterrows():
                        records.append({
                            "trade_date": str(row["日期"])[:10],
                            "market": "CN",
                            "symbol": symbol,
                            "name": name,
                            "open": row.get("开盘"),
                            "high": row.get("最高"),
                            "low": row.get("最低"),
                            "close": row.get("收盘"),
                            "volume": row.get("成交量"),
                            "turnover": row.get("成交额"),
                            "change_pct": row.get("涨跌幅"),
                            "degraded": False,
                        })
        except Exception as e:
            logger.error(f"AKShare采集失败: {e}")
            raise
        return records

    def store(self, records: list[dict]):
        stocks = [r for r in records if r.get("_type") != "northbound"]
        northbound = [r for r in records if r.get("_type") == "northbound"]

        if stocks:
            self.db.upsert_stock(stocks)
            logger.info(f"A股行情写入 {len(stocks)} 条")

        # 北向资金暂存到macro_indicators
        for nb in northbound:
            self.db.upsert_macro([{
                "time": nb["trade_date"] + "T00:00:00Z",
                "source_id": "TUSHARE",
                "series_id": "NORTH_NET",
                "series_name": "北向资金净流入(亿)",
                "value": nb.get("north_net", 0),
                "unit": "亿元",
                "period_type": "daily",
                "raw_url": "tushare",
                "raw_hash": self.make_hash(nb),
                "verified": True,
                "degraded": False,
            }])
