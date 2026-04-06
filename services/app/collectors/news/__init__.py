"""
新闻分层采集系统 - Professional News Tiered System

Tier 1: 直接价格影响 (EIA/IEA/OPEC报告、地缘冲突)
Tier 2: 政策/宏观 (央行政策、重大法规)
Tier 3: 市场情绪 (机构观点、次要数据)
"""

from .tiered_collector import TieredNewsCollector, NewsTier, ImpactType, NewsItem
from .narrative_tracker import NarrativeTracker, NARRATIVE_TEMPLATES
from .impact_analyzer import ImpactAnalyzer, CrossAssetValidation

__all__ = [
    "TieredNewsCollector", "NewsTier", "ImpactType", "NewsItem",
    "NarrativeTracker", "NARRATIVE_TEMPLATES",
    "ImpactAnalyzer", "CrossAssetValidation"
]
