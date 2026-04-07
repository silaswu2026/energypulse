"""
供应链数据采集模块

主方案: 妙想(mx-search) - 自然语言查询+正则提取
备选: 新浪财经API - BDRY ETF作为BDI proxy
"""

from .mx_supply_collector import MXSupplyChainCollector
from .shipping_index import ShippingIndexCollector
from .port_inventory import PortInventoryCollector
from .power_consumption import PowerConsumptionCollector

__all__ = [
    "MXSupplyChainCollector",
    "ShippingIndexCollector",
    "PortInventoryCollector",
    "PowerConsumptionCollector"
]
