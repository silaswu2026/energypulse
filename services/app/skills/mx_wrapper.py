"""
妙想Skills封装 - 为EnergyPulse提供A股数据接口
"""

import os
import sys
import json
import logging
from datetime import datetime

logger = logging.getLogger("energypulse.mx_skills")

# 确保API Key已设置
os.environ.setdefault("MX_APIKEY", "mkt_oto52LfYMH1rSIO6BLUgp5JbuwdYdfFsKNsfqQjeMKk")

SKILLS_DIR = "/app/skills"


def query_stock_price(stock_name: str) -> dict:
    """查询个股实时价格"""
    try:
        sys.path.insert(0, f"{SKILLS_DIR}/mx-data")
        from mx_data import call_mx_data_api
        
        query = f"{stock_name}最新价 涨跌幅"
        result = call_mx_data_api(query)
        
        return {
            "name": stock_name,
            "success": True,
            "data": result,
            "timestamp": datetime.utcnow().isoformat(),
        }
    except Exception as e:
        logger.error(f"查询{stock_name}价格失败: {e}")
        return {"name": stock_name, "success": False, "error": str(e)}


def query_sector_stocks(sector: str = "煤炭") -> list:
    """查询板块成分股"""
    try:
        sys.path.insert(0, f"{SKILLS_DIR}/mx-xuangu")
        from mx_xuangu import call_mx_xuangu_api
        
        query = f"{sector}板块成分股 按市值排序"
        result = call_mx_xuangu_api(query)
        
        return result if result else []
    except Exception as e:
        logger.error(f"查询{sector}板块失败: {e}")
        return []


def search_news(keyword: str, days: int = 1) -> list:
    """搜索资讯"""
    try:
        sys.path.insert(0, f"{SKILLS_DIR}/mx-search")
        from mx_search import call_mx_search_api
        
        query = f"{keyword} 近{days}天"
        result = call_mx_search_api(query)
        
        return result if result else []
    except Exception as e:
        logger.error(f"搜索{keyword}资讯失败: {e}")
        return []


def collect_cn_stocks_mx() -> list:
    """使用妙想采集A股能源板块数据（替代Tushare）"""
    logger.info("使用妙想Skills采集A股数据...")
    
    # 能源板块核心股票
    energy_stocks = [
        "中国神华", "陕西煤业", "兖州煤业", "中煤能源", "山西焦煤",
        "潞安环能", "平煤股份", "晋控煤业", "美锦能源", "华阳股份"
    ]
    
    records = []
    for stock in energy_stocks:
        try:
            data = query_stock_price(stock)
            if data.get("success"):
                # 解析返回数据
                price_data = data.get("data", {})
                if price_data:
                    records.append({
                        "symbol": stock,
                        "market": "CN",
                        "close": price_data.get("price"),
                        "change_pct": price_data.get("change_pct"),
                        "source": "mx-data",
                        "trade_date": datetime.now().strftime("%Y-%m-%d"),
                    })
        except Exception as e:
            logger.warning(f"采集{stock}失败: {e}")
    
    logger.info(f"妙想采集完成: {len(records)} 只股票")
    return records


if __name__ == "__main__":
    # 测试
    print("测试妙想Skills...")
    print(f"MX_APIKEY: {os.environ.get(MX_APIKEY, 未设置)[:10]}...")
    
    # 测试个股查询
    result = query_stock_price("宁德时代")
    print(f"\n宁德时代查询结果: {json.dumps(result, ensure_ascii=False, indent=2)[:500]}")
