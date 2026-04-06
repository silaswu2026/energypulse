"""
妙想Skills集成模块
为EnergyPulse提供A股数据和资讯查询
"""

from .mx_adapter import (
    query_cn_stock,
    query_cn_sector,
    search_cn_news,
    collect_cn_energy_stocks,
)

__all__ = [
    "query_cn_stock",
    "query_cn_sector", 
    "search_cn_news",
    "collect_cn_energy_stocks",
]
