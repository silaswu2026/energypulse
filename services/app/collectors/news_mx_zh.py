"""
妙想中文新闻采集器
覆盖: 国内能源政策、航运通道等中文新闻
"""

import sys
sys.path.insert(0, "/app")
sys.path.insert(0, "/app/skills/mx-search")

import logging
import json
from datetime import datetime
from database import get_db
from mx_search import MXSearch

logger = logging.getLogger("energypulse.news_mx_zh")

# 中文查询列表
ZH_QUERIES = [
    "霍尔木兹海峡船只通行伊朗协议",
    "国内航线燃油附加费发改委",
    "煤炭限价政策国内",
    "BDI指数波罗的海干散货",
    "港口煤炭库存最新",
]


class MXChineseNewsCollector:
    """妙想中文新闻采集器"""
    
    def __init__(self):
        self.client = MXSearch()
        self.db = get_db()
    
    def collect(self):
        """采集中文新闻"""
        records = []
        
        for query in ZH_QUERIES:
            try:
                result = self.client.search(query)
                
                if result.get("status") != 0:
                    continue
                
                content = MXSearch.extract_content(result)
                
                try:
                    data = json.loads(content)
                    items = data.get("data", [])
                except:
                    items = [{"title": content[:100], "content": content}]
                
                for item in items[:3]:
                    title = item.get("title", "")
                    summary = item.get("content", "")[:300]
                    category, impact_score = self._classify_news(title, summary)
                    
                    records.append({
                        "source": "mx_search_zh",
                        "title": title,
                        "summary": summary,
                        "url": item.get("url", ""),
                        "published_at": item.get("date", datetime.utcnow().isoformat()),
                        "language": "zh",
                        "category": category,
                        "impact_score": impact_score,
                        "query": query,
                    })
                
                logger.info(f"mx-search {query[:20]}...: {len(items)} 条")
                
            except Exception as e:
                logger.error(f"查询异常 {query}: {e}")
        
        return records
    
    def _classify_news(self, title: str, summary: str):
        """分类新闻并评分"""
        text = (title + " " + summary).lower()
        
        if any(kw in text for kw in ["霍尔木兹", "hormuz", "strait of hormuz"]):
            if any(pos in text for pos in ["恢复", "通行", "协议", "reopen"]):
                return "geopolitics", 7.0
            else:
                return "geopolitics", 9.0
        
        if any(kw in text for kw in ["燃油附加费", "航空煤油", "jet fuel surcharge"]):
            return "domestic_energy_policy", 5.5
        
        if any(kw in text for kw in ["煤炭限价", "coal price cap", "煤电"]):
            return "domestic_energy_policy", 6.5
        
        if any(kw in text for kw in ["bdi", "航运", "运价", "shipping"]):
            return "shipping_logistics", 5.0
        
        return "general", 3.0
    
    def save_to_db(self, records: list):
        """保存到数据库"""
        for r in records:
            try:
                sql = """
                    INSERT INTO news_sentiment 
                    (title, source, url, published_at, category, 
                     impact_score, language, summary, collected_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (url) DO UPDATE SET
                        impact_score = EXCLUDED.impact_score,
                        category = EXCLUDED.category
                """
                self.db.execute(sql, (
                    r["title"], r["source"], r["url"], r["published_at"],
                    r["category"], r["impact_score"], r["language"],
                    r["summary"], datetime.utcnow()
                ))
            except Exception as e:
                logger.error(f"保存失败: {e}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    collector = MXChineseNewsCollector()
    records = collector.collect()
    collector.save_to_db(records)
    print(f"采集并保存 {len(records)} 条中文新闻")
