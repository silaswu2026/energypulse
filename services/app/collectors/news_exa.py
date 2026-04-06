"""
Exa.ai 语义搜索新闻采集器
深度分析文章（非标题党）
"""

import time
import logging
from datetime import date, datetime, timedelta

from collectors.base import BaseCollector

logger = logging.getLogger("energypulse.news_exa")

# 语义搜索查询
SEMANTIC_QUERIES = [
    "impact of natural gas prices on coal power plant economics",
    "US coal export market Asia demand",
    "energy transition coal retirement clean energy",
    "China India coal consumption growth",
    "railroad logistics coal transportation bottlenecks",
]


class ExaNewsCollector(BaseCollector):
    """Exa.ai 语义搜索采集器"""

    def collect_primary(self) -> list[dict]:
        records = []
        start_date = (datetime.utcnow() - timedelta(days=2)).isoformat()
        
        for query in SEMANTIC_QUERIES:
            try:
                data = self.api_post("exa", "/search", json_body={
                    "query": query,
                    "type": "neural",
                    "numResults": 5,
                    "startPublishedDate": start_date,
                    "includeDomains": ["reuters.com", "bloomberg.com", "wsj.com", 
                                      "ft.com", "cnn.com", "cnbc.com",
                                      "eia.gov", "iea.org", "worldcoal.org"],
                })
                
                for result in data.get("results", []):
                    records.append({
                        "source": "exa_neural",
                        "title": result.get("title", ""),
                        "summary": result.get("text", "")[:500],
                        "url": result.get("url"),
                        "published_at": result.get("publishedDate", datetime.utcnow().isoformat()),
                        "language": "en",
                        "category": "deep_analysis",
                        "relevance": "high",
                        "sentiment_score": None,
                        "sentiment_label": None,
                        "raw_hash": self.make_hash(result),
                    })
                
                logger.info(f"Exa '{query[:30]}...': {len(data.get('results', []))} 条")
                time.sleep(1)
                
            except Exception as e:
                logger.warning(f"Exa 查询失败: {e}")
        
        return records
    
    def store(self, records: list[dict]):
        if records:
            self.db.upsert_news(records)
            logger.info(f"Exa新闻写入 {len(records)} 条")
