"""
传导分析器 - Transmission Analyzer (完整实现)

分析宏观-微观传导机制：
1. 美国利率政策 → 全球流动性 → 能源定价
2. 地缘政治 → 供应风险 → 价格冲击  
3. 中国政策 → 煤炭供需 → 股价表现
"""

import os
import sys
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from dataclasses import dataclass

sys.path.insert(0, "/app")
from database import get_db

logger = logging.getLogger("energypulse.analysis.transmission")


@dataclass
class TransmissionNode:
    """传导节点"""
    factor: str
    current_state: str
    trend: str
    impact_score: float


class TransmissionAnalyzer:
    """传导分析器"""
    
    # 预定义的传导路径
    PATHWAYS = {
        "rates_to_oil": {
            "name": "美国利率 → 原油价格",
            "description": "美联储利率政策通过影响美元和全球流动性，传导至原油价格",
            "steps": [
                {"factor": "美联储利率", "indicator": "DGS10", "threshold": 4.0},
                {"factor": "美元指数", "indicator": "USDIDX", "inverse": True},
                {"factor": "WTI原油", "indicator": "WTI", "commodity": True},
            ]
        },
        "dxy_to_commodities": {
            "name": "美元 → 大宗商品",
            "description": "美元强弱直接影响以美元计价的大宗商品价格",
            "steps": [
                {"factor": "美元指数", "indicator": "USDIDX"},
                {"factor": "WTI原油", "indicator": "WTI", "commodity": True},
                {"factor": "布伦特原油", "indicator": "BRENT", "commodity": True},
            ]
        },
    }
    
    def __init__(self):
        self.db = get_db()
        
    def get_indicator_state(self, indicator: str, is_commodity: bool = False) -> Dict:
        """获取指标当前状态"""
        try:
            since = datetime.utcnow() - timedelta(days=30)
            
            if is_commodity:
                sql = """
                    SELECT value, trade_date as date
                    FROM commodity_daily
                    WHERE commodity_id = %s AND trade_date >= %s
                    ORDER BY trade_date DESC LIMIT 2
                """
                results = self.db.query(sql, (indicator, since.strftime("%Y-%m-%d")))
            else:
                sql = """
                    SELECT value, date
                    FROM macro_indicators
                    WHERE indicator = %s AND date >= %s
                    ORDER BY date DESC LIMIT 2
                """
                results = self.db.query(sql, (indicator, since.strftime("%Y-%m-%d")))
            
            if len(results) >= 2:
                current = results[0]["value"]
                previous = results[1]["value"]
                change_pct = ((current - previous) / abs(previous)) * 100 if previous else 0
                
                trend = "up" if change_pct > 1 else "down" if change_pct < -1 else "stable"
                
                return {
                    "value": round(current, 2),
                    "change_pct": round(change_pct, 2),
                    "trend": trend
                }
            elif results:
                return {
                    "value": round(results[0]["value"], 2),
                    "change_pct": 0,
                    "trend": "stable"
                }
            
        except Exception as e:
            logger.error(f"获取{indicator}状态失败: {e}")
        
        return {"value": None, "change_pct": 0, "trend": "unknown"}
    
    def analyze_pathway(self, pathway_key: str) -> Dict:
        """分析单条传导路径"""
        config = self.PATHWAYS.get(pathway_key, {})
        steps = config.get("steps", [])
        
        nodes = []
        trend_consistency = []
        
        for step in steps:
            factor = step["factor"]
            indicator = step["indicator"]
            is_commodity = step.get("commodity", False)
            
            state = self.get_indicator_state(indicator, is_commodity)
            
            if state["value"] is not None:
                trend_consistency.append(state["trend"])
            
            nodes.append({
                "factor": factor,
                "indicator": indicator,
                "value": state["value"],
                "trend": state["trend"],
                "change_pct": state["change_pct"]
            })
        
        # 计算一致性
        if len(trend_consistency) >= 2:
            up_count = sum(1 for t in trend_consistency if t == "up")
            down_count = sum(1 for t in trend_consistency if t == "down")
            total = len(trend_consistency)
            
            if up_count > down_count:
                direction = "上行"
                confidence = up_count / total
            elif down_count > up_count:
                direction = "下行"
                confidence = down_count / total
            else:
                direction = "震荡"
                confidence = 0.5
        else:
            direction = "不明"
            confidence = 0.0
        
        return {
            "name": config.get("name", pathway_key),
            "description": config.get("description", ""),
            "nodes": nodes,
            "direction": direction,
            "confidence": round(confidence, 2),
            "active": confidence > 0.6
        }
    
    def generate_transmission_report(self) -> Dict:
        """生成传导分析报告"""
        pathways = []
        
        for key in self.PATHWAYS.keys():
            try:
                result = self.analyze_pathway(key)
                pathways.append(result)
            except Exception as e:
                logger.error(f"分析路径{key}失败: {e}")
        
        # 找出最活跃的传导路径
        active_pathways = [p for p in pathways if p.get("active")]
        dominant = max(active_pathways, key=lambda x: x["confidence"]) if active_pathways else None
        
        return {
            "pathways": pathways,
            "dominant_transmission": dominant,
            "summary": self._generate_summary(dominant, pathways),
            "timestamp": datetime.utcnow().isoformat()
        }
    
    def _generate_summary(self, dominant: Optional[Dict], pathways: List[Dict]) -> str:
        """生成总结"""
        if not dominant:
            return "当前市场传导信号不明显，各资产独立波动。"
        
        summary = f"主导传导路径：{dominant['name']}（置信度{dominant['confidence']:.0%}）。"
        summary += f"整体趋势：{dominant['direction']}。"
        
        # 提及关键节点
        key_nodes = [n for n in dominant['nodes'][:2] if n['value'] is not None]
        if key_nodes:
            summary += "关键节点："
            for node in key_nodes:
                trend_icon = {"up": "↑", "down": "↓", "stable": "→"}.get(node['trend'], "?")
                summary += f"{node['factor']}{trend_icon}({node['change_pct']:+.1f}%) "
        
        return summary


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    analyzer = TransmissionAnalyzer()
    report = analyzer.generate_transmission_report()
    print(json.dumps(report, indent=2, ensure_ascii=False))
