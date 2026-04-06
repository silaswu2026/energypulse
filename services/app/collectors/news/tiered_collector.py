"""
分层新闻采集器 - Tiered News Collector

三层架构：
- Tier 1: 高冲击事件 (冲击评分 7-10)
- Tier 2: 中冲击事件 (冲击评分 4-6)  
- Tier 3: 低冲击事件 (冲击评分 1-3)
"""

import os
import sys
import json
import logging
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, asdict
from enum import Enum

sys.path.insert(0, "/app")
from database import get_db

logger = logging.getLogger("energypulse.news.tiered")


class NewsTier(Enum):
    TIER1_CRITICAL = 1  # 直接价格影响
    TIER2_MACRO = 2     # 政策/宏观
    TIER3_SENTIMENT = 3 # 市场情绪


class ImpactType(Enum):
    SUPPLY_SHOCK = "supply_shock"       # 供应冲击
    DEMAND_SHOCK = "demand_shock"       # 需求冲击
    POLICY_CHANGE = "policy_change"     # 政策变化
    GEOPOLITICAL = "geopolitical"       # 地缘事件
    MARKET_STRUCTURE = "market_structure"  # 市场结构


@dataclass
class NewsItem:
    """新闻条目数据结构"""
    title: str
    source: str
    url: str
    published_at: datetime
    tier: NewsTier
    impact_score: float  # 1-10
    impact_type: Optional[ImpactType]
    category: str
    keywords: List[str]
    sentiment_score: float  # -1 to 1
    summary: str
    cross_asset_validation: Dict[str, Any]  # 跨市场验证
    raw_content: str = ""
    
    def to_dict(self) -> Dict:
        result = asdict(self)
        result['tier'] = self.tier.name
        result['impact_type'] = self.impact_type.value if self.impact_type else None
        result['published_at'] = self.published_at.isoformat()
        return result


# 事件冲击评分词典
EVENT_IMPACT_SCORES = {
    # 供应冲击 (最高优先级)
    "major_war": {"score": 10.0, "type": ImpactType.GEOPOLITICAL, "example": "俄乌战争爆发"},
    "supply_disruption_major": {"score": 9.0, "type": ImpactType.SUPPLY_SHOCK, "example": "主要管道中断"},
    "sanctions_major_exporter": {"score": 8.5, "type": ImpactType.GEOPOLITICAL, "example": "伊朗/俄罗斯制裁"},
    "opec_surprise_cut": {"score": 8.0, "type": ImpactType.SUPPLY_SHOCK, "example": "OPEC意外减产"},
    "major_field_outage": {"score": 7.5, "type": ImpactType.SUPPLY_SHOCK, "example": "北海油田停产"},
    
    # 政策变化
    "strategic_reserve_release": {"score": 7.0, "type": ImpactType.POLICY_CHANGE, "example": "美国释放SPR"},
    "china_production_policy": {"score": 6.5, "type": ImpactType.POLICY_CHANGE, "example": "中国限煤令"},
    "fed_rate_decision": {"score": 6.0, "type": ImpactType.MARKET_STRUCTURE, "example": "美联储加息"},
    "carbon_policy_major": {"score": 5.5, "type": ImpactType.POLICY_CHANGE, "example": "欧盟碳关税实施"},
    
    # 库存/数据
    "eia_large_miss": {"score": 6.0, "type": ImpactType.SUPPLY_SHOCK, "example": "EIA库存偏离预期500万桶+"},
    "inventory_crisis": {"score": 7.0, "type": ImpactType.SUPPLY_SHOCK, "example": "库欣库存告急"},
    
    # 需求冲击
    "china_stimulus_major": {"score": 6.5, "type": ImpactType.DEMAND_SHOCK, "example": "中国大规模刺激"},
    "global_recession_risk": {"score": 6.0, "type": ImpactType.DEMAND_SHOCK, "example": "全球经济衰退预警"},
}


class TieredNewsCollector:
    """分层新闻采集器"""
    
    def __init__(self):
        self.tavily_key = os.getenv("TAVILY_API_KEY", "")
        self.db = get_db()
        
    def detect_event_type(self, title: str, content: str) -> Optional[Dict]:
        """检测新闻事件类型和冲击评分"""
        text = (title + " " + content).lower()
        
        detected_events = []
        
        for event_key, event_info in EVENT_IMPACT_SCORES.items():
            keywords = self._get_event_keywords(event_key)
            if any(kw in text for kw in keywords):
                detected_events.append({
                    "event": event_key,
                    **event_info
                })
        
        if not detected_events:
            return None
            
        return max(detected_events, key=lambda x: x["score"])
    
    def _get_event_keywords(self, event_key: str) -> List[str]:
        """获取事件关键词"""
        keyword_map = {
            "major_war": ["war", "invasion", "attack", "missile", "军事冲突", "战争"],
            "supply_disruption_major": ["pipeline explosion", "force majeure", "supply disruption", "管道爆炸"],
            "sanctions_major_exporter": ["sanctions", "embargo", "制裁", "禁运"],
            "opec_surprise_cut": ["opec cut", "surprise cut", "减产", "unexpected reduction"],
            "eia_large_miss": ["inventory", "stock", "原油库存", "库存"],
            "china_production_policy": ["coal limit", "production cut", "限煤", "限产"],
            "fed_rate_decision": ["fed rate", "fomc", "加息", "降息"],
        }
        return keyword_map.get(event_key, [])
    
    def calculate_impact_score(self, title: str, content: str, source_tier: NewsTier) -> float:
        """计算综合冲击评分"""
        base_score = 5.0 if source_tier == NewsTier.TIER1 else \
                     3.0 if source_tier == NewsTier.TIER2 else 1.0
        
        event = self.detect_event_type(title, content)
        if event:
            base_score = max(base_score, event["score"])
        
        text = (title + " " + content).lower()
        urgency_markers = ["breaking", "urgent", "突发", "紧急"]
        if any(m in text for m in urgency_markers):
            base_score += 1.0
        
        return min(10.0, max(1.0, base_score))
    
    def search_tier1_news(self) -> List[NewsItem]:
        """搜索Tier 1高冲击新闻"""
        logger.info("搜索Tier 1高冲击新闻...")
        news_items = []
        
        critical_queries = [
            "war conflict Middle East oil supply disruption",
            "OPEC production cut announcement 2025",
            "major oil field outage pipeline explosion",
            "Iran sanctions oil export ban",
        ]
        
        for query in critical_queries:
            try:
                results = self._search_tavily(query, max_results=3)
                for r in results:
                    impact_score = self.calculate_impact_score(
                        r.get("title", ""), 
                        r.get("content", ""),
                        NewsTier.TIER1
                    )
                    
                    if impact_score >= 6.0:
                        event = self.detect_event_type(
                            r.get("title", ""), 
                            r.get("content", "")
                        )
                        
                        news = NewsItem(
                            title=r.get("title", "")[:200],
                            source=r.get("source", "Unknown"),
                            url=r.get("url", ""),
                            published_at=datetime.utcnow(),
                            tier=NewsTier.TIER1,
                            impact_score=impact_score,
                            impact_type=event["type"] if event else None,
                            category="geopolitics" if event and event["type"] == ImpactType.GEOPOLITICAL else "supply",
                            keywords=self._extract_keywords(r.get("title", "")),
                            sentiment_score=self._analyze_sentiment(r.get("content", "")),
                            summary=r.get("content", "")[:300],
                            cross_asset_validation={},
                            raw_content=r.get("content", "")
                        )
                        news_items.append(news)
                        
            except Exception as e:
                logger.error(f"搜索失败 {query}: {e}")
        
        logger.info(f"Tier 1新闻: {len(news_items)} 条")
        return news_items
    
    def search_tier2_news(self) -> List[NewsItem]:
        """搜索Tier 2政策宏观新闻"""
        logger.info("搜索Tier 2政策宏观新闻...")
        news_items = []
        
        policy_queries = [
            "Federal Reserve interest rate energy impact",
            "China coal production policy NDRC 2025",
            "EU carbon border tax CBAM energy",
            "US SPR strategic petroleum reserve release",
        ]
        
        for query in policy_queries:
            try:
                results = self._search_tavily(query, max_results=2)
                for r in results:
                    impact_score = self.calculate_impact_score(
                        r.get("title", ""),
                        r.get("content", ""),
                        NewsTier.TIER2
                    )
                    
                    news = NewsItem(
                        title=r.get("title", "")[:200],
                        source=r.get("source", "Unknown"),
                        url=r.get("url", ""),
                        published_at=datetime.utcnow(),
                        tier=NewsTier.TIER2,
                        impact_score=impact_score,
                        impact_type=ImpactType.POLICY_CHANGE,
                        category="policy",
                        keywords=self._extract_keywords(r.get("title", "")),
                        sentiment_score=self._analyze_sentiment(r.get("content", "")),
                        summary=r.get("content", "")[:300],
                        cross_asset_validation={},
                        raw_content=r.get("content", "")
                    )
                    news_items.append(news)
                    
            except Exception as e:
                logger.error(f"搜索失败 {query}: {e}")
        
        logger.info(f"Tier 2新闻: {len(news_items)} 条")
        return news_items
    
    def _search_tavily(self, query: str, max_results: int = 5) -> List[Dict]:
        """使用Tavily搜索"""
        if not self.tavily_key:
            return []
            
        url = "https://api.tavily.com/search"
        headers = {"Content-Type": "application/json"}
        payload = {
            "api_key": self.tavily_key,
            "query": query,
            "search_depth": "advanced",
            "max_results": max_results,
            "include_domains": [
                "reuters.com", "bloomberg.com", "ft.com", "wsj.com",
                "cnbc.com", "energyintel.com", "worldoil.com"
            ]
        }
        
        try:
            resp = requests.post(url, json=payload, headers=headers, timeout=30)
            data = resp.json()
            return data.get("results", [])
        except Exception as e:
            logger.error(f"Tavily搜索失败: {e}")
            return []
    
    def _extract_keywords(self, text: str) -> List[str]:
        """提取关键词"""
        energy_keywords = [
            "oil", "gas", "crude", "petroleum", "OPEC", "IEA", "EIA",
            "煤炭", "石油", "天然气", "能源", "原油", "汽油"
        ]
        found = [kw for kw in energy_keywords if kw.lower() in text.lower()]
        return found[:5]
    
    def _analyze_sentiment(self, text: str) -> float:
        """简单情绪分析"""
        positive = ["rise", "gain", "surge", "rally", "boost", "上涨", "增长"]
        negative = ["fall", "drop", "plunge", "crash", "decline", "下跌", "暴跌"]
        
        text_lower = text.lower()
        pos_count = sum(1 for p in positive if p in text_lower)
        neg_count = sum(1 for n in negative if n in text_lower)
        
        total = pos_count + neg_count
        if total == 0:
            return 0.0
        return (pos_count - neg_count) / total
    
    def save_to_database(self, news_items: List[NewsItem]):
        """保存新闻到数据库"""
        for item in news_items:
            try:
                sql = """
                    INSERT INTO news_sentiment 
                    (title, source, url, published_at, category, tier,
                     sentiment_score, sentiment_label, impact_score, 
                     impact_type, summary, collected_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (url) DO UPDATE SET
                        impact_score = EXCLUDED.impact_score,
                        tier = EXCLUDED.tier,
                        impact_type = EXCLUDED.impact_type
                """
                
                sentiment_label = "positive" if item.sentiment_score > 0.2 else \
                                 "negative" if item.sentiment_score < -0.2 else "neutral"
                
                self.db.execute(sql, (
                    item.title, item.source, item.url, item.published_at,
                    item.category, item.tier.name, item.sentiment_score,
                    sentiment_label, item.impact_score,
                    item.impact_type.value if item.impact_type else None,
                    item.summary, datetime.utcnow()
                ))
                
            except Exception as e:
                logger.error(f"保存新闻失败: {e}")
    
    def collect_all(self) -> Dict[str, List[NewsItem]]:
        """采集所有层级新闻"""
        logger.info("开始分层新闻采集...")
        
        tier1 = self.search_tier1_news()
        tier2 = self.search_tier2_news()
        
        all_news = tier1 + tier2
        self.save_to_database(all_news)
        
        return {
            "tier1": tier1,
            "tier2": tier2,
            "total": len(all_news)
        }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    collector = TieredNewsCollector()
    results = collector.collect_all()
    
    print(f"\n{'='*60}")
    print("分层新闻采集结果")
    print(f"{'='*60}")
    print(f"Tier 1 (高冲击): {len(results['tier1'])} 条")
    for n in results['tier1'][:3]:
        print(f"  [{n.impact_score:.1f}] {n.title[:60]}...")
    
    print(f"\nTier 2 (政策宏观): {len(results['tier2'])} 条")
    for n in results['tier2'][:3]:
        print(f"  [{n.impact_score:.1f}] {n.title[:60]}...")
