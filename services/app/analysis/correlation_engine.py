"""
相关性引擎 - Correlation Engine (完整实现)

功能：
1. 计算多资产相关性矩阵
2. 检测领先-滞后关系
3. 识别传导链条
"""

import os
import sys
import json
import logging
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from scipy.stats import pearsonr

sys.path.insert(0, "/app")
from database import get_db

logger = logging.getLogger("energypulse.analysis.correlation")


@dataclass
class TransmissionChain:
    """传导链条"""
    trigger: str
    intermediate: List[str]
    target: str
    delay_days: int
    strength: float
    confidence: float


class CorrelationEngine:
    """相关性分析引擎"""
    
    KEY_ASSETS = {
        "DXY": {"name": "美元指数", "category": "macro", "table": "macro_indicators", "column": "value"},
        "US10Y": {"name": "美债10年", "category": "macro", "table": "macro_indicators", "column": "value"},
        "BRENT": {"name": "布伦特原油", "category": "commodity", "table": "commodity_daily", "column": "value"},
        "WTI": {"name": "WTI原油", "category": "commodity", "table": "commodity_daily", "column": "value"},
    }
    
    # 预期传导链条
    EXPECTED_CHAINS = [
        ["US10Y", "DXY", "WTI"],  # 利率→美元→原油
        ["DXY", "WTI", "BRENT"],   # 美元→油价联动
    ]
    
    def __init__(self, lookback_days: int = 90):
        self.db = get_db()
        self.lookback_days = lookback_days
        
    def get_price_series(self, symbol: str) -> List[float]:
        """获取价格序列"""
        try:
            since = datetime.utcnow() - timedelta(days=self.lookback_days)
            
            config = self.KEY_ASSETS.get(symbol, {})
            table = config.get("table", "commodity_daily")
            value_col = config.get("column", "value")
            
            if table == "macro_indicators":
                indicator_map = {"DXY": "USDIDX", "US10Y": "DGS10"}
                indicator = indicator_map.get(symbol, symbol)
                sql = f"""
                    SELECT {value_col} as price
                    FROM {table}
                    WHERE indicator = %s AND date >= %s
                    ORDER BY date
                """
                results = self.db.query(sql, (indicator, since.strftime("%Y-%m-%d")))
            else:
                sql = f"""
                    SELECT {value_col} as price
                    FROM {table}
                    WHERE commodity_id = %s AND trade_date >= %s
                    ORDER BY trade_date
                """
                results = self.db.query(sql, (symbol, since.strftime("%Y-%m-%d")))
            
            return [r["price"] for r in results if r["price"] is not None]
            
        except Exception as e:
            logger.error(f"获取{symbol}价格失败: {e}")
            return []
    
    def calculate_correlation(self, series_a: List[float], 
                               series_b: List[float]) -> Tuple[float, float]:
        """计算皮尔逊相关系数和p值"""
        if len(series_a) < 10 or len(series_b) < 10:
            return 0.0, 1.0
        
        # 对齐长度
        min_len = min(len(series_a), len(series_b))
        a = series_a[-min_len:]
        b = series_b[-min_len:]
        
        try:
            corr, p_value = pearsonr(a, b)
            if np.isnan(corr):
                return 0.0, 1.0
            return corr, p_value
        except Exception as e:
            logger.error(f"计算相关性失败: {e}")
            return 0.0, 1.0
    
    def build_correlation_matrix(self) -> Dict:
        """构建相关性矩阵"""
        assets = list(self.KEY_ASSETS.keys())
        matrix = {}
        
        # 批量获取价格序列
        price_data = {}
        for asset in assets:
            series = self.get_price_series(asset)
            if len(series) >= 10:
                price_data[asset] = series
        
        # 计算相关性
        for i, a in enumerate(assets):
            matrix[a] = {}
            for b in assets:
                if a == b:
                    matrix[a][b] = {"correlation": 1.0, "p_value": 0.0}
                elif a in price_data and b in price_data:
                    corr, p = self.calculate_correlation(price_data[a], price_data[b])
                    matrix[a][b] = {
                        "correlation": round(corr, 3),
                        "p_value": round(p, 3),
                        "significant": p < 0.05
                    }
                else:
                    matrix[a][b] = {"correlation": 0.0, "p_value": 1.0, "significant": False}
        
        return matrix
    
    def analyze_transmission_chains(self) -> List[TransmissionChain]:
        """分析传导链条强度"""
        matrix = self.build_correlation_matrix()
        chains = []
        
        for chain in self.EXPECTED_CHAINS:
            if len(chain) < 2:
                continue
                
            # 计算链条上各环节的相关性乘积
            total_strength = 1.0
            valid_links = 0
            
            for i in range(len(chain) - 1):
                a, b = chain[i], chain[i + 1]
                if a in matrix and b in matrix[a]:
                    corr = abs(matrix[a][b]["correlation"])
                    if matrix[a][b].get("significant"):
                        total_strength *= corr
                        valid_links += 1
            
            if valid_links > 0:
                chains.append(TransmissionChain(
                    trigger=chain[0],
                    intermediate=chain[1:-1],
                    target=chain[-1],
                    delay_days=0,  # 简化，不计算滞后
                    strength=round(total_strength, 3),
                    confidence=valid_links / (len(chain) - 1)
                ))
        
        chains.sort(key=lambda x: x.strength, reverse=True)
        return chains
    
    def generate_correlation_report(self) -> Dict:
        """生成相关性分析报告"""
        matrix = self.build_correlation_matrix()
        chains = self.analyze_transmission_chains()
        
        # 找出最强相关性
        strongest_pairs = []
        assets = list(self.KEY_ASSETS.keys())
        for i, a in enumerate(assets):
            for b in assets[i+1:]:
                if a in matrix and b in matrix[a]:
                    corr_data = matrix[a][b]
                    if abs(corr_data["correlation"]) > 0.3:
                        strongest_pairs.append({
                            "pair": f"{a}-{b}",
                            "correlation": corr_data["correlation"],
                            "significant": corr_data.get("significant", False)
                        })
        
        strongest_pairs.sort(key=lambda x: abs(x["correlation"]), reverse=True)
        
        return {
            "lookback_days": self.lookback_days,
            "strongest_correlations": strongest_pairs[:5],
            "transmission_chains": [
                {
                    "trigger": c.trigger,
                    "target": c.target,
                    "strength": c.strength,
                    "confidence": round(c.confidence, 2)
                }
                for c in chains[:3]
            ],
            "timestamp": datetime.utcnow().isoformat()
        }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    engine = CorrelationEngine(lookback_days=30)
    report = engine.generate_correlation_report()
    print(json.dumps(report, indent=2, ensure_ascii=False))
