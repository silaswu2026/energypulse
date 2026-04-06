"""
冲击分析器 - Impact Analyzer

功能：
1. 跨市场验证（Cross-Asset Validation）
2. 价格冲击检测
3. 事件影响评估
"""

import os
import sys
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass

sys.path.insert(0, "/app")
from database import get_db

logger = logging.getLogger("energypulse.news.impact")


@dataclass
class CrossAssetValidation:
    """跨市场验证结果"""
    oil_price_change: float  # 油价变化%
    energy_equity_change: float  # 能源股变化%
    inflation_expectation: float  # 通胀预期变化
    usd_index_change: float  # 美元指数变化
    correlation_score: float  # 相关性评分 0-1
    is_validated: bool  # 是否通过验证


class ImpactAnalyzer:
    """冲击分析器"""
    
    def __init__(self):
        self.db = get_db()
        
    def get_price_changes(self, hours: int = 2) -> Dict[str, float]:
        """获取多市场价格变化"""
        try:
            since = datetime.utcnow() - timedelta(hours=hours)
            
            # 获取油价变化（WTI）
            oil_sql = """
                SELECT value FROM commodity_daily 
                WHERE commodity_id = 'WTI' 
                AND trade_date >= %s::date - INTERVAL '1 day'
                ORDER BY trade_date DESC LIMIT 2
            """
            oil_result = self.db.query(oil_sql, (since,))
            oil_change = 0.0
            if len(oil_result) >= 2:
                latest = oil_result[0]['value']
                previous = oil_result[1]['value']
                oil_change = ((latest - previous) / previous) * 100 if previous else 0
            
            # 获取能源股变化（XLE ETF）
            equity_sql = """
                SELECT close_price FROM stock_daily 
                WHERE symbol = 'XLE' 
                AND trade_date >= %s::date - INTERVAL '1 day'
                ORDER BY trade_date DESC LIMIT 2
            """
            equity_result = self.db.query(equity_sql, (since,))
            equity_change = 0.0
            if len(equity_result) >= 2:
                latest = equity_result[0]['close_price']
                previous = equity_result[1]['close_price']
                equity_change = ((latest - previous) / previous) * 100 if previous else 0
            
            # 获取美元指数
            dxy_sql = """
                SELECT value FROM macro_indicators 
                WHERE indicator = 'DXY' 
                AND date >= %s::date - INTERVAL '1 day'
                ORDER BY date DESC LIMIT 2
            """
            dxy_result = self.db.query(dxy_sql, (since,))
            dxy_change = 0.0
            if len(dxy_result) >= 2:
                latest = dxy_result[0]['value']
                previous = dxy_result[1]['value']
                dxy_change = ((latest - previous) / previous) * 100 if previous else 0
            
            return {
                "oil_change": oil_change,
                "energy_equity_change": equity_change,
                "dxy_change": dxy_change,
            }
            
        except Exception as e:
            logger.error(f"获取价格变化失败: {e}")
            return {
                "oil_change": 0.0,
                "energy_equity_change": 0.0,
                "dxy_change": 0.0,
            }
    
    def validate_news_impact(self, news_impact_score: float, 
                             time_window_hours: int = 2) -> CrossAssetValidation:
        """
        验证新闻冲击是否被价格确认
        
        逻辑：
        - 高冲击新闻(>7)应该伴随>1%的价格变动
        - 油价和能源股应该同向变动（验证一致性）
        - 如果油价涨但能源股跌，可能是假突破
        """
        prices = self.get_price_changes(time_window_hours)
        
        oil_chg = prices["oil_change"]
        equity_chg = prices["energy_equity_change"]
        dxy_chg = prices["dxy_change"]
        
        # 计算相关性
        # 正常情况下，油价和能源股应该正相关
        if abs(oil_chg) > 0.5 and abs(equity_chg) > 0.5:
            same_direction = (oil_chg > 0) == (equity_chg > 0)
            correlation = 0.8 if same_direction else 0.2
        else:
            correlation = 0.5  # 数据不足
        
        # 验证规则
        is_validated = False
        
        if news_impact_score >= 8.0:
            # 极高冲击：应该看到>2%的价格变动
            is_validated = abs(oil_chg) > 2.0 or abs(equity_chg) > 2.0
        elif news_impact_score >= 6.0:
            # 高冲击：应该看到>1%的价格变动
            is_validated = abs(oil_chg) > 1.0 or abs(equity_chg) > 1.0
        elif news_impact_score >= 4.0:
            # 中等冲击：应该看到>0.5%的价格变动
            is_validated = abs(oil_chg) > 0.5 or abs(equity_chg) > 0.5
        else:
            # 低冲击：不要求价格验证
            is_validated = True
        
        # 异常检测：油价和能源股背离
        divergence_warning = abs(oil_chg - equity_chg) > 2.0
        
        return CrossAssetValidation(
            oil_price_change=oil_chg,
            energy_equity_change=equity_chg,
            inflation_expectation=0.0,  # 可从TIPS利差计算
            usd_index_change=dxy_chg,
            correlation_score=correlation,
            is_validated=is_validated and not divergence_warning
        )
    
    def calculate_expected_impact(self, event_type: str, 
                                   severity: str = "medium") -> Dict:
        """
        计算事件的预期市场影响
        
        基于历史类似事件的平均影响
        """
        impact_matrix = {
            "supply_shock": {
                "high": {"oil": "+5-10%", "duration": "1-2 weeks", "equity": "+3-5%"},
                "medium": {"oil": "+2-5%", "duration": "3-5 days", "equity": "+1-3%"},
                "low": {"oil": "+1-2%", "duration": "1-2 days", "equity": "+0-1%"},
            },
            "demand_shock": {
                "high": {"oil": "-10-15%", "duration": "2-4 weeks", "equity": "-5-8%"},
                "medium": {"oil": "-3-7%", "duration": "1-2 weeks", "equity": "-2-4%"},
                "low": {"oil": "-1-3%", "duration": "2-3 days", "equity": "-0-2%"},
            },
            "policy_change": {
                "high": {"oil": "±3-5%", "duration": "1 week", "equity": "±2-4%"},
                "medium": {"oil": "±1-3%", "duration": "2-3 days", "equity": "±1-2%"},
                "low": {"oil": "±0-1%", "duration": "1 day", "equity": "±0-1%"},
            },
            "geopolitical": {
                "high": {"oil": "+5-15%", "duration": "2-4 weeks", "equity": "volatile"},
                "medium": {"oil": "+2-5%", "duration": "1 week", "equity": "±2%"},
                "low": {"oil": "+1-2%", "duration": "1-2 days", "equity": "±1%"},
            },
        }
        
        return impact_matrix.get(event_type, {}).get(severity, {
            "oil": "unknown", "duration": "unknown", "equity": "unknown"
        })
    
    def generate_impact_report(self, news_item: Dict) -> str:
        """为单条新闻生成冲击分析报告"""
        title = news_item.get("title", "")
        impact_score = news_item.get("impact_score", 5.0)
        event_type = news_item.get("impact_type", "unknown")
        
        # 验证
        validation = self.validate_news_impact(impact_score)
        
        # 预期影响
        severity = "high" if impact_score >= 8 else "medium" if impact_score >= 5 else "low"
        expected = self.calculate_expected_impact(event_type, severity)
        
        report = f"""
## 冲击分析: {title[:50]}...

**冲击评分**: {impact_score:.1f}/10  
**事件类型**: {event_type}  
**价格验证**: {"✅ 已确认" if validation.is_validated else "⚠️ 待确认"}

**跨市场验证**:
- 油价变化: {validation.oil_price_change:+.2f}%
- 能源股变化: {validation.energy_equity_change:+.2f}%
- 美元指数变化: {validation.usd_index_change:+.2f}%
- 相关性评分: {validation.correlation_score:.0%}

**预期影响** (基于历史类似事件):
- 油价预期: {expected.get('oil', 'unknown')}
- 能源股预期: {expected.get('equity', 'unknown')}
- 影响持续时间: {expected.get('duration', 'unknown')}

**交易启示**:
{"价格已反映新闻冲击，追涨风险较高。" if validation.is_validated else "价格尚未充分反映，关注后续走势。"}
{"油价与能源股背离，警惕假突破。" if abs(validation.oil_price_change - validation.energy_equity_change) > 2 else "各市场方向一致，趋势可信。"}
"""
        return report


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    analyzer = ImpactAnalyzer()
    
    # 模拟测试
    test_news = {
        "title": "OPEC announces surprise production cut of 1 million barrels",
        "impact_score": 8.5,
        "impact_type": "supply_shock"
    }
    
    report = analyzer.generate_impact_report(test_news)
    print(report)
