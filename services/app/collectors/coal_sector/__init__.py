"""
煤炭产业链细分采集器

覆盖：
- 动力煤（发电）
- 焦煤（钢铁）
- 无烟煤（化工）
- 煤层气/煤化工
"""

from .coal_chain_collector import CoalChainCollector
from .coal_stock_collector import CoalStockCollector

__all__ = ["CoalChainCollector", "CoalStockCollector"]
