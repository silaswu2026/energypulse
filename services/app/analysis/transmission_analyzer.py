"""
传导分析器 - Transmission Analyzer (修复版)
"""

import os
import sys
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
    
    PATHWAYS = {
        "rates_to_oil": {
            "name": "美国利率 → 原油价格",
            "description": "美联储利率政策通过影响美元和全球流动性，传导至原油价格",
            "steps": [
                {"factor": "美联储利率", "series_id": "DGS10", "is_macro": True},
                {"factor": "美元指数", "series_id": "DTWEXBGS", "is_macro": True},
                {"factor": "WTI原油", "commodity_id": "WTI", "is_commodity": True},
            ]
        },
        "dxy_to_commodities": {
            "name": "美元 → 大宗商品",
            "description": "美元强弱直接影响以美元计价的大宗商品价格",
            "steps": [
                {"factor": "美元指数", "series_id": "DTWEXBGS", "is_macro": True},
                {"factor": "WTI原油", "commodity_id": "WTI", "is_commodity": True},
                {"factor": "布伦特原油", "commodity_id": "BRENT", "is_commodity": True},
            ]
        },
    }
    
    def __init__(self):
        self.db = get_db()
        
    def get_macro_state(self, series_id: str) -> Dict:
        """获取宏观指标状态"""
        try:
            since = datetime.utcnow() - timedelta(days=60)
            
            sql = """
                SELECT value, time
                FROM macro_indicators
                WHERE series_id = %s AND time >= %s
                ORDER BY time DESC LIMIT 2
            """
            results = self.db.query(sql, (series_id, since))
            
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
            logger.error(f"获取{macro_state}失败: {e}")
        
        return {"value": None, "change_pct": 0, "trend": "unknown"}
    
    def get_commodity_state(self, commodity_id: str) -> Dict:
        """获取商品状态"""
        try:
            sql = """
                SELECT value, change_pct, trade_date
                FROM commodity_daily
                WHERE commodity_id = %s
                ORDER BY trade_date DESC LIMIT 1
            """
            results = self.db.query(sql, (commodity_id,))
            
            if results:
                return {
                    "value": results[0]["value"],
                    "change_pct": results[0].get("change_pct") or 0,
                    "trend": "up" if (results[0].get("change_pct") or 0) > 1 else "down" if (results[0].get("change_pct") or 0) < -1 else "stable"
                }
        except Exception as e:
            logger.error(f"获取{commodity_id}状态失败: {e}")
        
        return {"value": None, "change_pct": 0, "trend": "unknown"}
    
    def analyze_pathway(self, pathway_key: str) -> Dict:
        """分析单条传导路径"""
        config = self.PATHWAYS.get(pathway_key, {})
        steps = config.get("steps", [])
        
        nodes = []
        trend_consistency = []
        
        for step in steps:
            factor = step["factor"]
            
            if step.get("is_macro"):
                series_id = step.get("series_id")
                state = self.get_macro_state(series_id)
            elif step.get("is_commodity"):
                commodity_id = step.get("commodity_id")
                state = self.get_commodity_state(commodity_id)
            else:
                state = {"value": None, "trend": "unknown"}
            
            if state["value"] is not None:
                trend_consistency.append(state["trend"])
            
            nodes.append({
                "factor": factor,
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
        
        summary = "传导分析完成"
        if dominant:
            summary = f"主导传导: {dominant[name]}，方向{dominant[direction]}，置信度{dominant[confidence]:.0%}"
        
        return {
            "pathways": pathways,
            "dominant_transmission": dominant,
            "summary": summary,
            "timestamp": datetime.utcnow().isoformat()
        }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    analyzer = TransmissionAnalyzer()
    report = analyzer.generate_transmission_report()
    print(report)
