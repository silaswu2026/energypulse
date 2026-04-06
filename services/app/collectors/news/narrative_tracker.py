"""
叙事跟踪系统 - Narrative Tracking System

跟踪市场主导叙事的变化，识别：
1. 当前主导叙事
2. 叙事强度（一致性、持续性）
3. 叙事切换信号
"""

import os
import sys
import json
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
from collections import defaultdict
from dataclasses import dataclass

sys.path.insert(0, "/app")
from database import get_db

logger = logging.getLogger("energypulse.news.narrative")


# 预定义的市场叙事模板
NARRATIVE_TEMPLATES = {
    "supply_fear": {
        "name": "供应恐惧叙事",
        "description": "地缘政治冲突、制裁、OPEC减产导致的供应担忧",
        "keywords": ["supply disruption", "sanctions", "OPEC cut", "war", "conflict", 
                     "供应中断", "制裁", "减产", "战争", "冲突"],
        "indicators": ["油价上涨", "能源股涨", "波动率升"],
        "typical_duration_days": 14,
        "impact_level": "high"
    },
    "demand_recovery": {
        "name": "需求复苏叙事",
        "description": "中国经济刺激、全球制造业复苏带动的需求预期",
        "keywords": ["China stimulus", "demand recovery", "manufacturing PMI", "growth",
                     "中国刺激", "需求复苏", "制造业", "增长"],
        "indicators": ["油价稳涨", "煤炭需求升", "运价涨"],
        "typical_duration_days": 30,
        "impact_level": "medium"
    },
    "policy_transition": {
        "name": "能源转型叙事",
        "description": "碳中和政策、可再生能源替代对传统能源的冲击",
        "keywords": ["carbon neutral", "energy transition", "renewable", "ESG", "coal phase out",
                     "碳中和", "能源转型", "可再生能源", "退煤"],
        "indicators": ["传统能源股跌", "新能源股涨", "碳价涨"],
        "typical_duration_days": 60,
        "impact_level": "medium"
    },
    "recession_fear": {
        "name": "衰退恐惧叙事",
        "description": "美联储加息、全球经济放缓导致的需求担忧",
        "keywords": ["recession", "Fed rate", "slowdown", "demand destruction",
                     "衰退", "加息", "放缓", "需求破坏"],
        "indicators": ["油价跌", "全市场跌", "美元涨"],
        "typical_duration_days": 21,
        "impact_level": "high"
    },
    "inventory_glut": {
        "name": "库存过剩叙事",
        "description": "EIA库存持续累积，市场供应过剩",
        "keywords": ["inventory build", "surplus", "oversupply", "stockpile",
                     "库存增加", "过剩", "供应过剩"],
        "indicators": ["油价跌", "月差收窄", "存储费涨"],
        "typical_duration_days": 14,
        "impact_level": "medium"
    },
}


@dataclass
class NarrativeState:
    """叙事状态"""
    narrative_id: str
    name: str
    strength: float  # 0-1, 叙事强度
    consistency: float  # 0-1, 一致性（新闻情绪同向程度）
    duration_days: int  # 持续天数
    news_count: int  # 相关新闻数量
    avg_impact_score: float  # 平均冲击评分
    last_updated: datetime
    is_dominant: bool  # 是否为主导叙事


class NarrativeTracker:
    """叙事跟踪器"""
    
    def __init__(self, lookback_days: int = 7):
        self.db = get_db()
        self.lookback_days = lookback_days
        
    def get_recent_news(self) -> List[Dict]:
        """获取近期新闻"""
        try:
            sql = """
                SELECT title, content, sentiment_score, impact_score, 
                       impact_type, category, published_at, tier
                FROM news_sentiment
                WHERE collected_at > NOW() - INTERVAL '%s days'
                ORDER BY impact_score DESC
            """
            return self.db.query(sql, (self.lookback_days,))
        except Exception as e:
            logger.error(f"获取新闻失败: {e}")
            return []
    
    def match_narratives(self, news_items: List[Dict]) -> Dict[str, List[Dict]]:
        """将新闻匹配到叙事模板"""
        narrative_matches = defaultdict(list)
        
        for news in news_items:
            text = (news.get("title", "") + " " + news.get("content", "")).lower()
            
            for narrative_id, template in NARRATIVE_TEMPLATES.items():
                keywords = template["keywords"]
                match_score = sum(1 for kw in keywords if kw.lower() in text)
                
                if match_score >= 1:  # 至少匹配一个关键词
                    narrative_matches[narrative_id].append({
                        **news,
                        "match_score": match_score
                    })
        
        return dict(narrative_matches)
    
    def calculate_narrative_strength(self, matched_news: List[Dict]) -> Tuple[float, float]:
        """计算叙事强度和一致性"""
        if not matched_news:
            return 0.0, 0.0
        
        # 强度 = 新闻数量 * 平均冲击评分
        news_count = len(matched_news)
        avg_impact = sum(n.get("impact_score", 5) for n in matched_news) / news_count
        strength = min(1.0, (news_count / 10) * (avg_impact / 10))
        
        # 一致性 = 情绪方向的一致性
        sentiments = [n.get("sentiment_score", 0) for n in matched_news]
        if not sentiments:
            consistency = 0.0
        else:
            # 计算情绪方差，方差越小一致性越高
            avg_sentiment = sum(sentiments) / len(sentiments)
            variance = sum((s - avg_sentiment) ** 2 for s in sentiments) / len(sentiments)
            consistency = max(0.0, 1.0 - variance)
        
        return strength, consistency
    
    def analyze_narratives(self) -> List[NarrativeState]:
        """分析当前市场叙事"""
        news_items = self.get_recent_news()
        narrative_matches = self.match_narratives(news_items)
        
        states = []
        for narrative_id, template in NARRATIVE_TEMPLATES.items():
            matched = narrative_matches.get(narrative_id, [])
            
            if not matched:
                continue
                
            strength, consistency = self.calculate_narrative_strength(matched)
            
            # 计算持续时间
            dates = [n.get("published_at") for n in matched if n.get("published_at")]
            if dates:
                duration = (max(dates) - min(dates)).days if isinstance(max(dates), datetime) else 1
            else:
                duration = 1
            
            avg_impact = sum(n.get("impact_score", 5) for n in matched) / len(matched)
            
            state = NarrativeState(
                narrative_id=narrative_id,
                name=template["name"],
                strength=strength,
                consistency=consistency,
                duration_days=duration,
                news_count=len(matched),
                avg_impact_score=avg_impact,
                last_updated=datetime.utcnow(),
                is_dominant=False  # 稍后确定
            )
            states.append(state)
        
        # 标记主导叙事（强度最高的）
        if states:
            strongest = max(states, key=lambda x: x.strength)
            for s in states:
                s.is_dominant = (s.narrative_id == strongest.narrative_id)
        
        # 按强度排序
        states.sort(key=lambda x: x.strength, reverse=True)
        
        return states
    
    def detect_narrative_shift(self, current: List[NarrativeState], 
                                previous: List[NarrativeState]) -> Optional[Dict]:
        """检测叙事切换"""
        if not current or not previous:
            return None
        
        current_dominant = next((n for n in current if n.is_dominant), None)
        previous_dominant = next((n for n in previous if n.is_dominant), None)
        
        if not current_dominant or not previous_dominant:
            return None
        
        if current_dominant.narrative_id != previous_dominant.narrative_id:
            return {
                "type": "narrative_shift",
                "from": previous_dominant.name,
                "to": current_dominant.name,
                "shift_strength": current_dominant.strength - previous_dominant.strength,
                "timestamp": datetime.utcnow().isoformat()
            }
        
        # 检查同一叙事的强度突变
        strength_change = current_dominant.strength - previous_dominant.strength
        if abs(strength_change) > 0.3:
            return {
                "type": "strength_change",
                "narrative": current_dominant.name,
                "change": strength_change,
                "timestamp": datetime.utcnow().isoformat()
            }
        
        return None
    
    def generate_narrative_report(self) -> Dict[str, Any]:
        """生成叙事分析报告"""
        states = self.analyze_narratives()
        
        dominant = next((n for n in states if n.is_dominant), None)
        
        report = {
            "dominant_narrative": {
                "id": dominant.narrative_id if dominant else None,
                "name": dominant.name if dominant else "无明显叙事",
                "strength": round(dominant.strength, 2) if dominant else 0,
                "duration_days": dominant.duration_days if dominant else 0,
            },
            "active_narratives": [
                {
                    "id": s.narrative_id,
                    "name": s.name,
                    "strength": round(s.strength, 2),
                    "consistency": round(s.consistency, 2),
                    "news_count": s.news_count,
                    "avg_impact": round(s.avg_impact_score, 1),
                }
                for s in states[:3]  # 前3个活跃叙事
            ],
            "narrative_summary": self._generate_summary(states),
            "timestamp": datetime.utcnow().isoformat()
        }
        
        return report
    
    def _generate_summary(self, states: List[NarrativeState]) -> str:
        """生成叙事总结"""
        if not states:
            return "近期市场缺乏明确叙事，处于震荡整理阶段。"
        
        dominant = states[0]
        template = NARRATIVE_TEMPLATES.get(dominant.narrative_id, {})
        
        summary = f"当前市场由「{dominant.name}」主导，强度{dominant.strength:.0%}。"
        summary += f"该叙事已持续{dominant.duration_days}天，"
        
        if dominant.consistency > 0.7:
            summary += "市场情绪高度一致，"
        elif dominant.consistency > 0.4:
            summary += "市场情绪较为分化，"
        else:
            summary += "市场情绪混乱，"
        
        summary += template.get("description", "")
        
        # 提及次要叙事
        if len(states) > 1:
            secondary = states[1]
            summary += f" 次要叙事为「{secondary.name}」({secondary.strength:.0%})。"
        
        return summary


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    tracker = NarrativeTracker(lookback_days=7)
    report = tracker.generate_narrative_report()
    
    print("\n" + "="*60)
    print("市场叙事分析报告")
    print("="*60)
    print(f"\n主导叙事: {report['dominant_narrative']['name']}")
    print(f"叙事强度: {report['dominant_narrative']['strength']}")
    print(f"\n叙事总结:\n{report['narrative_summary']}")
    print(f"\n活跃叙事:")
    for n in report['active_narratives']:
        print(f"  - {n['name']}: 强度{n['strength']}, 相关新闻{n['news_count']}条")
