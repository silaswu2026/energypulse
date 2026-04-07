"""
告警管理器 - Alert Manager

功能：
1. 持续监控数据
2. 触发告警
3. 通知分发
4. 告警历史管理
"""

import os
import sys
import json
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from dataclasses import dataclass, asdict

sys.path.insert(0, "/app")
from database import get_db
from alerts.alert_rules import AlertRules, AlertRule, AlertSeverity, AlertCategory

logger = logging.getLogger("energypulse.alerts")


@dataclass
class Alert:
    """告警实例"""
    id: str
    rule_id: str
    title: str
    message: str
    severity: AlertSeverity
    category: AlertCategory
    triggered_at: datetime
    acknowledged: bool = False
    data_snapshot: Dict = None


class AlertManager:
    """告警管理器"""
    
    def __init__(self):
        self.db = get_db()
        self.rules = AlertRules()
        self._init_database()
    
    def _init_database(self):
        """初始化告警表"""
        try:
            # 告警历史表已在外部SQL创建
            pass
        except Exception as e:
            logger.error(f"初始化告警表失败: {e}")
    
    def check_price_alerts(self) -> List[Alert]:
        """检查价格异动告警"""
        alerts = []
        
        try:
            # 检查WTI价格变化
            sql = """
                SELECT value, change_pct, trade_date
                FROM commodity_daily
                WHERE commodity_id = 'WTI'
                ORDER BY trade_date DESC LIMIT 1
            """
            result = self.db.query(sql)
            
            if result:
                change_pct = result[0].get("change_pct", 0)
                
                # 检查暴涨
                if change_pct >= 5.0:
                    rule = self.rules.get_rule("price_oil_spike_5pct")
                    if rule and rule.enabled and not self._is_in_cooldown(rule.id):
                        alerts.append(Alert(
                            id=f"oil_spike_{datetime.utcnow().strftime('%Y%m%d%H%M')}",
                            rule_id=rule.id,
                            title="🚨 原油暴涨5%+",
                            message=f"WTI原油单日上涨{change_pct:.2f}%，触发价格异动告警",
                            severity=AlertSeverity.HIGH,
                            category=AlertCategory.PRICE_SPIKE,
                            triggered_at=datetime.utcnow(),
                            data_snapshot={"change_pct": change_pct, "price": result[0]["value"]}
                        ))
                
                # 检查暴跌
                elif change_pct <= -5.0:
                    rule = self.rules.get_rule("price_oil_crash_5pct")
                    if rule and rule.enabled and not self._is_in_cooldown(rule.id):
                        alerts.append(Alert(
                            id=f"oil_crash_{datetime.utcnow().strftime('%Y%m%d%H%M')}",
                            rule_id=rule.id,
                            title="🚨 原油暴跌5%+",
                            message=f"WTI原油单日下跌{abs(change_pct):.2f}%，触发价格异动告警",
                            severity=AlertSeverity.HIGH,
                            category=AlertCategory.PRICE_SPIKE,
                            triggered_at=datetime.utcnow(),
                            data_snapshot={"change_pct": change_pct, "price": result[0]["value"]}
                        ))
        
        except Exception as e:
            logger.error(f"检查价格告警失败: {e}")
        
        return alerts
    
    def check_news_alerts(self) -> List[Alert]:
        """检查新闻告警"""
        alerts = []
        
        try:
            # 检查高冲击新闻
            sql = """
                SELECT title, impact_score, impact_type, tier
                FROM news_sentiment
                WHERE tier = 'TIER1_CRITICAL'
                AND impact_score >= 9.0
                AND collected_at > NOW() - INTERVAL '1 hour'
                ORDER BY impact_score DESC LIMIT 5
            """
            results = self.db.query(sql)
            
            for news in results:
                # 战争冲突
                if any(kw in news["title"].lower() for kw in ["war", "attack", "invasion", "战争"]):
                    rule = self.rules.get_rule("geo_war_outbreak")
                    if rule and rule.enabled and not self._is_in_cooldown(rule.id):
                        alerts.append(Alert(
                            id=f"war_{datetime.utcnow().strftime('%Y%m%d%H%M')}",
                            rule_id=rule.id,
                            title="🔴 CRITICAL: 战争冲突爆发",
                            message=f"检测到战争冲突新闻: {news['title'][:80]}...",
                            severity=AlertSeverity.CRITICAL,
                            category=AlertCategory.GEOPOLITICAL,
                            triggered_at=datetime.utcnow(),
                            data_snapshot={"impact_score": news["impact_score"], "title": news["title"]}
                        ))
                
                # 制裁
                elif "sanctions" in news["title"].lower() or "制裁" in news["title"]:
                    rule = self.rules.get_rule("geo_major_sanctions")
                    if rule and rule.enabled and not self._is_in_cooldown(rule.id):
                        alerts.append(Alert(
                            id=f"sanctions_{datetime.utcnow().strftime('%Y%m%d%H%M')}",
                            rule_id=rule.id,
                            title="⚠️ 重大制裁措施",
                            message=f"检测到制裁新闻: {news['title'][:80]}...",
                            severity=AlertSeverity.HIGH,
                            category=AlertCategory.GEOPOLITICAL,
                            triggered_at=datetime.utcnow(),
                            data_snapshot={"impact_score": news["impact_score"]}
                        ))
        
        except Exception as e:
            logger.error(f"检查新闻告警失败: {e}")
        
        return alerts
    
    def _is_in_cooldown(self, rule_id: str) -> bool:
        """检查是否在冷却期"""
        try:
            rule = self.rules.get_rule(rule_id)
            if not rule:
                return False
            
            sql = """
                SELECT triggered_at
                FROM alerts
                WHERE rule_id = %s
                ORDER BY triggered_at DESC LIMIT 1
            """
            result = self.db.query(sql, (rule_id,))
            
            if result:
                last_triggered = result[0]["triggered_at"]
                cooldown_end = last_triggered + timedelta(minutes=rule.cooldown_minutes)
                return datetime.utcnow() < cooldown_end
            
        except Exception as e:
            logger.error(f"检查冷却期失败: {e}")
        
        return False
    
    def save_alert(self, alert: Alert):
        """保存告警"""
        try:
            sql = """
                INSERT INTO alerts (id, rule_id, title, message, severity, 
                                   category, triggered_at, data_snapshot)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
            """
            self.db.execute(sql, (
                alert.id, alert.rule_id, alert.title, alert.message,
                alert.severity.value, alert.category.value,
                alert.triggered_at, json.dumps(alert.data_snapshot or {})
            ))
        except Exception as e:
            logger.error(f"保存告警失败: {e}")
    
    def get_recent_alerts(self, hours: int = 24) -> List[Dict]:
        """获取近期告警"""
        try:
            sql = """
                SELECT id, title, message, severity, category, 
                       triggered_at, acknowledged
                FROM alerts
                WHERE triggered_at > NOW() - INTERVAL '%s hours'
                ORDER BY triggered_at DESC
            """
            return self.db.query(sql, (hours,))
        except Exception as e:
            logger.error(f"获取告警历史失败: {e}")
            return []
    
    def format_alert_for_display(self, alert: Alert) -> str:
        """格式化告警用于展示"""
        severity_icons = {
            AlertSeverity.CRITICAL: "🔴",
            AlertSeverity.HIGH: "🟠",
            AlertSeverity.MEDIUM: "🟡",
            AlertSeverity.LOW: "🔵"
        }
        
        icon = severity_icons.get(alert.severity, "⚪")
        return f"""
{icon} [{alert.severity.value.upper()}] {alert.title}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
{alert.message}

时间: {alert.triggered_at.strftime('%Y-%m-%d %H:%M UTC')}
类别: {alert.category.value}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
    
    def check_all(self) -> List[Alert]:
        """检查所有告警"""
        all_alerts = []
        
        all_alerts.extend(self.check_price_alerts())
        all_alerts.extend(self.check_news_alerts())
        
        # 保存告警
        for alert in all_alerts:
            self.save_alert(alert)
        
        return all_alerts


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    manager = AlertManager()
    alerts = manager.check_all()
    
    print(f"\n检测到 {len(alerts)} 条告警:\n")
    for alert in alerts:
        print(manager.format_alert_for_display(alert))
