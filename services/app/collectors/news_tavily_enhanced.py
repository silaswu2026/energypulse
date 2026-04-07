"""
Tavily新闻采集器 - 增强版
覆盖: 原有能源新闻 + 航运通道 + 国内能源政策
"""

from collectors.news_tavily import TavilyNewsCollector, QUERIES
import logging

logger = logging.getLogger("energypulse.news_tavily_enhanced")

# 扩展查询列表
ENHANCED_QUERIES = QUERIES + [
    {"query": "Strait of Hormuz shipping traffic Iran maritime", "topic": "geopolitics", "depth": "advanced"},
    {"query": "global shipping supply chain disruption logistics", "topic": "shipping_logistics", "depth": "advanced"},
    {"query": "China jet fuel surcharge aviation fuel price NDRC", "topic": "domestic_energy_policy", "depth": "advanced"},
    {"query": "China coal price cap policy NDRC energy regulation", "topic": "domestic_energy_policy", "depth": "advanced"},
]


class EnhancedTavilyNewsCollector(TavilyNewsCollector):
    """增强版Tavily采集器"""
    
    def collect_primary(self):
        """使用增强的查询列表"""
        records = []
        
        for q in ENHANCED_QUERIES:
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
                    impact_score = self._detect_impact_event(
                        result.get("title", ""), 
                        result.get("content", "")
                    )
                    
                    records.append({
                        "source": f"tavily_{q[topic]}",
                        "title": result.get("title", ""),
                        "summary": result.get("content", "")[:500],
                        "url": result.get("url"),
                        "published_at": self._parse_date(result.get("published_date")),
                        "language": "en",
                        "category": q["topic"],
                        "relevance": "high" if q["depth"] == "advanced" else "medium",
                        "impact_score": impact_score,
                        "raw_hash": self.make_hash(result),
                    })
                
                logger.info(f"Tavily {q[topic]}: {len(data.get(results, []))} 条")
                
            except Exception as e:
                logger.warning(f"Tavily {q[topic]} 失败: {e}")
        
        return records
    
    def _detect_impact_event(self, title: str, content: str) -> float:
        """检测是否是高冲击事件"""
        text = (title + " " + content).lower()
        
        high_impact_keywords = {
            "strait of hormuz": 9.0,
            "hormuz": 8.5,
            "霍尔木兹": 8.5,
            "shipping lane closure": 8.0,
            "maritime blockage": 8.0,
            "supply chain crisis": 7.5,
            "port closure": 7.0,
            "fuel surcharge increase": 5.5,
            "coal price cap": 6.5,
        }
        
        max_score = 0
        for keyword, score in high_impact_keywords.items():
            if keyword.lower() in text:
                max_score = max(max_score, score)
        
        return max_score
