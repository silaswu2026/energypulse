"""
告警规则定义
"""

from enum import Enum
from dataclasses import dataclass
from typing import List, Optional


class AlertSeverity(Enum):
    CRITICAL = "critical"  # 立即通知
    HIGH = "high"          # 1小时内通知
    MEDIUM = "medium"      # 4小时内通知
    LOW = "low"            # 日报汇总


class AlertCategory(Enum):
    GEOPOLITICAL = "geopolitical"
    PRICE_SPIKE = "price_spike"
    INVENTORY_MISS = "inventory_miss"
    POLICY_CHANGE = "policy_change"
    FUND_FLOW = "fund_flow"


@dataclass
class AlertRule:
    """告警规则"""
    id: str
    name: str
    category: AlertCategory
    severity: AlertSeverity
    condition: str  # 条件描述
    threshold: float
    cooldown_minutes: int  # 冷却时间
    enabled: bool = True


# 预定义告警规则
DEFAULT_ALERT_RULES = [
    # 地缘冲突
    AlertRule(
        id="geo_war_outbreak",
        name="战争爆发",
        category=AlertCategory.GEOPOLITICAL,
        severity=AlertSeverity.CRITICAL,
        condition="tier1 news with war/conflict keywords",
        threshold=9.0,  # impact_score
        cooldown_minutes=60
    ),
    AlertRule(
        id="geo_major_sanctions",
        name="重大制裁",
        category=AlertCategory.GEOPOLITICAL,
        severity=AlertSeverity.HIGH,
        condition="sanctions on major oil exporter",
        threshold=8.0,
        cooldown_minutes=120
    ),
    
    # 价格异动
    AlertRule(
        id="price_oil_spike_5pct",
        name="原油暴涨5%+",
        category=AlertCategory.PRICE_SPIKE,
        severity=AlertSeverity.HIGH,
        condition="WTI daily change > 5%",
        threshold=5.0,
        cooldown_minutes=30
    ),
    AlertRule(
        id="price_oil_crash_5pct",
        name="原油暴跌5%+",
        category=AlertCategory.PRICE_SPIKE,
        severity=AlertSeverity.HIGH,
        condition="WTI daily change < -5%",
        threshold=-5.0,
        cooldown_minutes=30
    ),
    AlertRule(
        id="price_coal_spike_3pct",
        name="煤炭暴涨3%+",
        category=AlertCategory.PRICE_SPIKE,
        severity=AlertSeverity.MEDIUM,
        condition="Coal ETF daily change > 3%",
        threshold=3.0,
        cooldown_minutes=60
    ),
    
    # 库存偏离
    AlertRule(
        id="inv_eia_large_miss",
        name="EIA库存大幅偏离",
        category=AlertCategory.INVENTORY_MISS,
        severity=AlertSeverity.HIGH,
        condition="EIA inventory miss > 5 million barrels",
        threshold=500.0,  # 万桶
        cooldown_minutes=240
    ),
    
    # 政策变化
    AlertRule(
        id="policy_china_coal",
        name="中国煤炭政策突变",
        category=AlertCategory.POLICY_CHANGE,
        severity=AlertSeverity.HIGH,
        condition="NDRC coal policy announcement",
        threshold=7.0,
        cooldown_minutes=180
    ),
]


class AlertRules:
    """告警规则管理"""
    
    def __init__(self):
        self.rules = {r.id: r for r in DEFAULT_ALERT_RULES}
    
    def get_enabled_rules(self) -> List[AlertRule]:
        """获取启用的规则"""
        return [r for r in self.rules.values() if r.enabled]
    
    def get_rule(self, rule_id: str) -> Optional[AlertRule]:
        """获取规则"""
        return self.rules.get(rule_id)
    
    def disable_rule(self, rule_id: str):
        """禁用规则"""
        if rule_id in self.rules:
            self.rules[rule_id].enabled = False
    
    def enable_rule(self, rule_id: str):
        """启用规则"""
        if rule_id in self.rules:
            self.rules[rule_id].enabled = True


if __name__ == "__main__":
    rules = AlertRules()
    print(f"已加载 {len(rules.get_enabled_rules())} 条告警规则")
    for r in rules.get_enabled_rules():
        print(f"  [{r.severity.value}] {r.name}")
