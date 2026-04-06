"""
商品数据收集器 - 使用yfinance（免费，无需API Key）
获取原油、黄金、天然气、VIX等价格数据
"""

import logging
from datetime import date, datetime
import yfinance as yf
from database import get_db

logger = logging.getLogger("energypulse.commodity_yf")

# Yahoo Finance 代码映射
COMMODITY_SYMBOLS = {
    "CL=F": "WTI原油",      # WTI原油
    "BZ=F": "布伦特原油",   # 布伦特原油
    "NG=F": "天然气",       # Henry Hub天然气
    "GC=F": "黄金",         # COMEX黄金
    "HG=F": "铜",           # COMEX铜
    "^VIX": "VIX",          # 波动率指数
    "DX-Y.NYB": "美元指数",  # 美元指数
}


def collect_commodities():
    """收集商品数据"""
    logger.info("开始收集商品数据(yfinance)...")
    
    db = get_db()
    today = date.today().isoformat()
    records = []
    
    for symbol, name in COMMODITY_SYMBOLS.items():
        try:
            # 获取最新数据
            ticker = yf.Ticker(symbol)
            hist = ticker.history(period="2d")
            
            if len(hist) < 2:
                logger.warning(f"{symbol} 数据不足")
                continue
            
            # 计算涨跌幅
            latest = hist.iloc[-1]
            prev = hist.iloc[-2]
            
            change_pct = ((latest["Close"] - prev["Close"]) / prev["Close"]) * 100
            
            record = {
                "commodity_id": name,
                "symbol": symbol,
                "trade_date": today,
                "value": round(latest["Close"], 2),
                "change_pct": round(change_pct, 2),
                "source": "yfinance",
                "created_at": datetime.utcnow().isoformat(),
            }
            records.append(record)
            
            logger.info(f"{name}: {record[value]} (Change: {record[change_pct]}%)")
            
        except Exception as e:
            logger.error(f"获取 {symbol} 失败: {e}")
    
    # 存入数据库
    if records:
        for r in records:
            sql = """
                INSERT INTO commodity_daily (commodity_id, trade_date, value, change_pct, source, created_at)
                VALUES (%(commodity_id)s, %(trade_date)s, %(value)s, %(change_pct)s, %(source)s, %(created_at)s)
                ON CONFLICT (commodity_id, trade_date)
                DO UPDATE SET value = EXCLUDED.value,
                              change_pct = EXCLUDED.change_pct,
                              source = EXCLUDED.source
            """
            db.execute(sql, r)
        logger.info(f"商品数据已保存: {len(records)} 条")
    
    return records


if __name__ == "__main__":
    # 测试
    records = collect_commodities()
    print(f"\n收集完成，共 {len(records)} 条数据")
    for r in records:
        print(f"  {r[commodity_id]}: {r[value]} ({r[change_pct]}%)")
