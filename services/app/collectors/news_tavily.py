"""
Tavily AI新闻搜索采集器
华尔街能源前沿新闻
"""

import time
import logging
from datetime import date, datetime, timedelta

from collectors.base import BaseCollector

logger = logging.getLogger("energypulse.news_tavily")

# 搜索查询列表
QUERIES = [
    {"query": "coal energy market outlook price", "topic": "coal_market", "depth": "advanced"},
    {"query": "US power generation coal natural gas electricity", "topic": "power", "depth": "advanced"},
    {"query": "EIA energy outlook coal production inventory", "topic": "eia_report", "depth": "basic"},
    {"query": "China coal import demand energy security", "topic": "china_coal", "depth": "basic"},
    {"query": "OPEC oil production energy geopolitics", "topic": "geopolitics", "depth": "basic"},
    {"query": "renewable energy transition coal phase out", "topic": "policy", "depth": "basic"},
]


class TavilyNewsCollector(BaseCollector):
    """Tavily AI新闻采集器"""

    def collect_primary(self) -> list[dict]:
        records = []
        
        for q in QUERIES:
            try:
                data = self.api_post("tavily", "/search", json_body={
                    "api_key": self.config.get_key("tavily"),
                    "query": q["query"],
                    "search_depth": q["depth"],
                    "max_results": 5,
                    "include_answer": False,
                    "include_raw_content": False,
                })
                
                for result in data.get("results", []):
                    records.append({
                        "source": f"tavily_{q['topic']}",
                        "title": result.get("title", ""),
                        "summary": result.get("content", "")[:500],
                        "url": result.get("url"),
                        "published_at": self._parse_date(result.get("published_date")),
                        "language": "en",
                        "category": q["topic"],
                        "relevance": "high" if q["depth"] == "advanced" else "medium",
                        "sentiment_score": None,
                        "sentiment_label": None,
                        "raw_hash": self.make_hash(result),
                    })
                
                logger.info(f"Tavily {q['topic']}: {len(data.get('results', []))} 条")
                time.sleep(1)  # 控制频率
                
            except Exception as e:
                logger.warning(f"Tavily {q['topic']} 失败: {e}")
        
        return records
    
    def _parse_date(self, date_str):
        """解析日期字符串"""
        if not date_str:
            return datetime.utcnow().isoformat()
        try:
            # 尝试 ISO 格式
            return datetime.fromisoformat(date_str.replace('Z', '+00:00')).isoformat()
        except:
            return datetime.utcnow().isoformat()
    
    def store(self, records: list[dict]):
        if records:
            self.db.upsert_news(records)
            logger.info(f"Tavily新闻写入 {len(records)} 条")
