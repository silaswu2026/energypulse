"""
相关性引擎 - Correlation Engine (修复版)
"""

import os
import sys
import logging
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
from dataclasses import dataclass
from scipy.stats import pearsonr

sys.path.insert(0, "/app")
from database import get_db

logger = logging.getLogger("energypulse.analysis.correlation")


@dataclass
class TransmissionChain:
    trigger: str
    intermediate: List[str]
    target: str
    delay_days: int
    strength: float
    confidence: float


class CorrelationEngine:
    KEY_ASSETS = {
        "DXY": {"name": "美元指数", "table": "macro_indicators", "column": "value", "id_field": "series_id", "id_value": "DTWEXBGS"},
        "US10Y": {"name": "美债10年", "table": "macro_indicators", "column": "value", "id_field": "series_id", "id_value": "DGS10"},
        "BRENT": {"name": "布伦特原油", "table": "commodity_daily", "column": "value", "id_field": "commodity_id", "id_value": "BRENT"},
        "WTI": {"name": "WTI原油", "table": "commodity_daily", "column": "value", "id_field": "commodity_id", "id_value": "WTI"},
    }
    
    EXPECTED_CHAINS = [
        ["US10Y", "DXY", "WTI"],
        ["DXY", "WTI", "BRENT"],
    ]
    
    def __init__(self, lookback_days: int = 90):
        self.db = get_db()
        self.lookback_days = lookback_days
        
    def get_price_series(self, symbol: str) -> List[float]:
        try:
            since = datetime.utcnow() - timedelta(days=self.lookback_days)
            config = self.KEY_ASSETS.get(symbol, {})
            table = config.get("table", "commodity_daily")
            value_col = config.get("column", "value")
            id_field = config.get("id_field", "commodity_id")
            id_value = config.get("id_value", symbol)
            
            if table == "macro_indicators":
                sql = f"""
                    SELECT {value_col} as price
                    FROM {table}
                    WHERE {id_field} = %s AND time >= %s
                    ORDER BY time
                """
            else:
                sql = f"""
                    SELECT {value_col} as price
                    FROM {table}
                    WHERE {id_field} = %s AND trade_date >= %s
                    ORDER BY trade_date
                """
            
            results = self.db.query(sql, (id_value, since.strftime("%Y-%m-%d")))
            return [r["price"] for r in results if r["price"] is not None]
            
        except Exception as e:
            logger.error(f"获取{symbol}价格失败: {e}")
            return []
    
    def calculate_correlation(self, series_a: List[float], series_b: List[float]) -> Tuple[float, float]:
        if len(series_a) < 10 or len(series_b) < 10:
            return 0.0, 1.0
        
        min_len = min(len(series_a), len(series_b))
        a = np.array(series_a[-min_len:])
        b = np.array(series_b[-min_len:])
        
        try:
            corr, p_value = pearsonr(a, b)
            if np.isnan(corr):
                return 0.0, 1.0
            return corr, p_value
        except Exception as e:
            logger.error(f"计算相关性失败: {e}")
            return 0.0, 1.0
    
    def build_correlation_matrix(self) -> Dict:
        assets = list(self.KEY_ASSETS.keys())
        matrix = {}
        
        price_data = {}
        for asset in assets:
            series = self.get_price_series(asset)
            if len(series) >= 10:
                price_data[asset] = series
        
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
    
    def generate_correlation_report(self) -> Dict:
        matrix = self.build_correlation_matrix()
        
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
            "timestamp": datetime.utcnow().isoformat()
        }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    engine = CorrelationEngine(lookback_days=30)
    report = engine.generate_correlation_report()
    print(report)
