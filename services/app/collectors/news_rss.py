"""
RSS订阅新闻采集器
EIA, E&E News, Reuters等
"""

import time
import logging
import feedparser
from datetime import datetime

from collectors.base import BaseCollector

logger = logging.getLogger("energypulse.news_rss")

# RSS源列表
RSS_FEEDS = [
    {
        "name": "EIA Today in Energy",
        "url": "https://www.eia.gov/todayinenergy/rss.xml",
        "category": "eia_official",
    },
    {
        "name": "EIA Petroleum",
        "url": "https://www.eia.gov/petroleum/weekly/rss.xml",
        "category": "eia_oil",
    },
    {
        "name": "EIA Natural Gas",
        "url": "https://www.eia.gov/naturalgas/weekly/rss.xml",
        "category": "eia_gas",
    },
]


class RSSNewsCollector(BaseCollector):
    """RSS新闻采集器"""

    def collect_primary(self) -> list[dict]:
        records = []
        
        for feed_info in RSS_FEEDS:
            try:
                feed = feedparser.parse(feed_info["url"])
                
                for entry in feed.entries[:10]:  # 每个源取最近10条
                    published = self._get_published_date(entry)
                    
                    records.append({
                        "source": f"rss_{feed_info['category']}",
                        "title": entry.get("title", ""),
                        "summary": entry.get("summary", "")[:500],
                        "url": entry.get("link"),
                        "published_at": published,
                        "language": "en",
                        "category": feed_info["category"],
                        "relevance": "medium",
                        "sentiment_score": None,
                        "sentiment_label": None,
                        "raw_hash": self.make_hash({
                            "title": entry.get("title"),
                            "url": entry.get("link"),
                            "published": published,
                        }),
                    })
                
                logger.info(f"RSS {feed_info['name']}: {len(feed.entries)} 条")
                time.sleep(0.5)
                
            except Exception as e:
                logger.warning(f"RSS {feed_info['name']} 失败: {e}")
        
        return records
    
    def _get_published_date(self, entry) -> str:
        """提取发布日期"""
        for key in ["published", "updated", "created"]:
            if hasattr(entry, key):
                try:
                    # feedparser 会自动解析为 time.struct_time
                    if hasattr(entry, f"{key}_parsed"):
                        parsed = getattr(entry, f"{key}_parsed")
                        return datetime(*parsed[:6]).isoformat()
                except:
                    pass
        return datetime.utcnow().isoformat()
    
    def store(self, records: list[dict]):
        if records:
            self.db.upsert_news(records)
            logger.info(f"RSS新闻写入 {len(records)} 条")
