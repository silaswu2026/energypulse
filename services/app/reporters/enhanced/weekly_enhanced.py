"""
增强版周报生成器

相比基础版新增：
1. 本周核心叙事回顾与演变
2. 资金流向分析（北向资金、主力动向）
3. 机构观点汇总（投行评级、目标价）
4. 交易机会雷达（多维度评分）
"""

import os
import sys
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from dataclasses import dataclass

sys.path.insert(0, "/app")
sys.path.insert(0, "/app/collectors/news")
sys.path.insert(0, "/app/analysis")

from database import get_db
from ai.llm_client import LLMClient
from narrative_tracker import NarrativeTracker
from correlation_engine import CorrelationEngine

logger = logging.getLogger("energypulse.reporter.weekly_enhanced")


@dataclass
class WeeklySummary:
    """周报摘要数据"""
    week_range: str
    dominant_narrative: str
    narrative_evolution: str
    key_events: List[Dict]
    market_performance: Dict
    fund_flow: Dict
    institutional_views: List[Dict]
    opportunities: List[Dict]


class EnhancedWeeklyReporter:
    """增强版周报生成器"""
    
    def __init__(self):
        self.db = get_db()
        self.llm = LLMClient()
        self.narrative_tracker = NarrativeTracker(lookback_days=7)
        self.correlation_engine = CorrelationEngine(lookback_days=30)
        
    def get_week_range(self) -> str:
        """获取本周日期范围"""
        today = datetime.now()
        monday = today - timedelta(days=today.weekday())
        friday = monday + timedelta(days=4)
        return f"{monday.strftime('%m/%d')}-{friday.strftime('%m/%d')}"
    
    def get_market_performance(self) -> Dict:
        """获取本周市场表现"""
        try:
            week_start = datetime.now() - timedelta(days=7)
            
            # 美股能源
            us_energy = self._get_ticker_performance("XLE", week_start)
            # 中国煤炭
            cn_coal = self._get_ticker_performance("煤炭ETF", week_start)
            # WTI原油
            wti = self._get_commodity_performance("WTI", week_start)
            # 美元指数
            dxy = self._get_macro_performance("DXY", week_start)
            
            return {
                "us_energy": us_energy,
                "cn_coal": cn_coal,
                "wti": wti,
                "dxy": dxy
            }
        except Exception as e:
            logger.error(f"获取市场表现失败: {e}")
            return {}
    
    def _get_ticker_performance(self, symbol: str, since: datetime) -> Dict:
        """获取股票表现"""
        sql = """
            SELECT close_price, trade_date
            FROM stock_daily
            WHERE symbol = %s AND trade_date >= %s
            ORDER BY trade_date
        """
        results = self.db.query(sql, (symbol, since.strftime("%Y-%m-%d")))
        
        if len(results) >= 2:
            first = results[0]["close_price"]
            last = results[-1]["close_price"]
            change_pct = ((last - first) / first) * 100
            return {
                "change_pct": round(change_pct, 2),
                "trend": "up" if change_pct > 0 else "down"
            }
        return {"change_pct": 0, "trend": "flat"}
    
    def _get_commodity_performance(self, commodity: str, since: datetime) -> Dict:
        """获取商品表现"""
        sql = """
            SELECT value, trade_date
            FROM commodity_daily
            WHERE commodity_id = %s AND trade_date >= %s
            ORDER BY trade_date
        """
        results = self.db.query(sql, (commodity, since.strftime("%Y-%m-%d")))
        
        if len(results) >= 2:
            first = results[0]["value"]
            last = results[-1]["value"]
            change_pct = ((last - first) / first) * 100
            return {
                "change_pct": round(change_pct, 2),
                "current": last
            }
        return {"change_pct": 0, "current": None}
    
    def _get_macro_performance(self, indicator: str, since: datetime) -> Dict:
        """获取宏观指标表现"""
        indicator_map = {"DXY": "USDIDX"}
        ind = indicator_map.get(indicator, indicator)
        
        sql = """
            SELECT value, date
            FROM macro_indicators
            WHERE indicator = %s AND date >= %s
            ORDER BY date
        """
        results = self.db.query(sql, (ind, since.strftime("%Y-%m-%d")))
        
        if len(results) >= 2:
            first = results[0]["value"]
            last = results[-1]["value"]
            change_pct = ((last - first) / first) * 100
            return {
                "change_pct": round(change_pct, 2),
                "current": last
            }
        return {"change_pct": 0, "current": None}
    
    def get_fund_flow(self) -> Dict:
        """获取资金流向"""
        try:
            # 北向资金流向（如果有数据）
            sql = """
                SELECT value, date
                FROM macro_indicators
                WHERE indicator = 'NORTHBOUND_FLOW'
                AND date >= NOW() - INTERVAL '7 days'
                ORDER BY date
            """
            northbound = self.db.query(sql)
            
            # XLE ETF资金流向
            sql2 = """
                SELECT value, date
                FROM macro_indicators
                WHERE indicator = 'XLE_FLOW'
                AND date >= NOW() - INTERVAL '7 days'
                ORDER BY date
            """
            xle_flow = self.db.query(sql2)
            
            return {
                "northbound": {
                    "net_flow": sum(r["value"] for r in northbound) if northbound else 0,
                    "unit": "亿元"
                },
                "xle_etf": {
                    "net_flow": sum(r["value"] for r in xle_flow) if xle_flow else 0,
                    "unit": "百万美元"
                },
                "interpretation": self._interpret_fund_flow(northbound, xle_flow)
            }
        except Exception as e:
            logger.error(f"获取资金流向失败: {e}")
            return {"note": "资金流向数据暂缺"}
    
    def _interpret_fund_flow(self, northbound, xle_flow) -> str:
        """解读资金流向"""
        if not northbound and not xle_flow:
            return "暂无资金流向数据"
        
        interpretations = []
        
        if northbound:
            total = sum(r["value"] for r in northbound)
            if total > 50:
                interpretations.append("北向资金大幅流入A股，外资看好中国能源板块")
            elif total < -50:
                interpretations.append("北向资金流出，外资短期避险")
        
        if xle_flow:
            total = sum(r["value"] for r in xle_flow)
            if total > 100:
                interpretations.append("美股能源ETF资金流入，全球能源受青睐")
        
        return "；".join(interpretations) if interpretations else "资金流向平稳"
    
    def get_institutional_views(self) -> List[Dict]:
        """获取机构观点汇总"""
        # 从新闻中提取机构观点
        try:
            sql = """
                SELECT title, source, sentiment_score
                FROM news_sentiment
                WHERE category IN ('market', 'policy')
                AND source IN ('Bloomberg', 'Reuters', 'Goldman Sachs', 'Morgan Stanley')
                AND collected_at > NOW() - INTERVAL '7 days'
                ORDER BY impact_score DESC
                LIMIT 5
            """
            views = self.db.query(sql)
            
            return [
                {
                    "institution": v["source"],
                    "view": v["title"][:100],
                    "sentiment": "bullish" if v["sentiment_score"] > 0.2 else "bearish" if v["sentiment_score"] < -0.2 else "neutral"
                }
                for v in views
            ]
        except Exception as e:
            logger.error(f"获取机构观点失败: {e}")
            return []
    
    def build_opportunity_radar(self) -> List[Dict]:
        """构建交易机会雷达"""
        opportunities = []
        
        # 1. 宏观驱动机会
        macro_score = self._score_macro_opportunity()
        if macro_score["total"] > 60:
            opportunities.append({
                "type": "宏观驱动",
                "theme": "美国利率见顶利好高股息",
                "score": macro_score["total"],
                "factors": macro_score["factors"],
                "time_horizon": "3-6个月",
                "risk_level": "中"
            })
        
        # 2. 季节性机会
        season_score = self._score_seasonal_opportunity()
        if season_score["total"] > 50:
            opportunities.append({
                "type": "季节性",
                "theme": "迎峰度夏需求高峰",
                "score": season_score["total"],
                "factors": season_score["factors"],
                "time_horizon": "1-2个月",
                "risk_level": "低"
            })
        
        # 3. 事件驱动机会
        event_score = self._score_event_opportunity()
        if event_score["total"] > 55:
            opportunities.append({
                "type": "事件驱动",
                "theme": "地缘冲突推升能源风险溢价",
                "score": event_score["total"],
                "factors": event_score["factors"],
                "time_horizon": "2-4周",
                "risk_level": "高"
            })
        
        # 按评分排序
        opportunities.sort(key=lambda x: x["score"], reverse=True)
        return opportunities
    
    def _score_macro_opportunity(self) -> Dict:
        """评分宏观机会"""
        factors = []
        total = 50
        
        # 检查利率趋势
        rates = self._get_macro_performance("US10Y", datetime.now() - timedelta(days=30))
        if rates.get("change_pct", 0) < -5:
            factors.append("美债收益率下行，利好高股息")
            total += 15
        
        # 检查美元
        dxy = self._get_macro_performance("DXY", datetime.now() - timedelta(days=30))
        if dxy.get("change_pct", 0) < -2:
            factors.append("美元走弱，大宗商品受益")
            total += 10
        
        return {"total": min(100, total), "factors": factors}
    
    def _score_seasonal_opportunity(self) -> Dict:
        """评分季节性机会"""
        factors = []
        total = 40
        
        month = datetime.now().month
        if month in [6, 7]:
            factors.append("夏季用电高峰临近")
            total += 25
        elif month in [11, 12]:
            factors.append("冬季供暖季开始")
            total += 25
        
        return {"total": min(100, total), "factors": factors}
    
    def _score_event_opportunity(self) -> Dict:
        """评分事件驱动机会"""
        factors = []
        total = 40
        
        # 检查是否有高冲击新闻
        try:
            sql = """
                SELECT COUNT(*) as count
                FROM news_sentiment
                WHERE tier = 'TIER1_CRITICAL'
                AND impact_score >= 8
                AND collected_at > NOW() - INTERVAL '7 days'
            """
            result = self.db.query(sql)
            if result and result[0]["count"] > 0:
                factors.append(f"本周有{result[0]['count']}个高冲击事件")
                total += 20
        except:
            pass
        
        return {"total": min(100, total), "factors": factors}
    
    def generate(self) -> Dict:
        """生成增强版周报"""
        logger.info("生成增强版周报...")
        
        # 收集数据
        week_range = self.get_week_range()
        narrative = self.narrative_tracker.generate_narrative_report()
        performance = self.get_market_performance()
        fund_flow = self.get_fund_flow()
        inst_views = self.get_institutional_views()
        opportunities = self.build_opportunity_radar()
        
        # 构建AI提示
        prompt = self._build_prompt(
            week_range, narrative, performance, 
            fund_flow, inst_views, opportunities
        )
        
        # 生成内容
        content_md = self.llm.call("weekly_enhanced", prompt)
        
        # 保存报告
        self._save_report(week_range, content_md, {
            "narrative": narrative,
            "performance": performance,
            "fund_flow": fund_flow,
            "opportunities": opportunities
        })
        
        return {
            "week": week_range,
            "content": content_md,
            "data": {
                "narrative": narrative,
                "performance": performance,
                "opportunities": opportunities
            }
        }
    
    def _build_prompt(self, week_range, narrative, performance, 
                      fund_flow, inst_views, opportunities) -> str:
        """构建AI提示"""
        return f"""作为能源投资研究专家，撰写一份专业周报。

## 本周区间: {week_range}

## 核心叙事
{narrative.get('narrative_summary', '暂无')}

主导叙事: {narrative.get('dominant_narrative', {}).get('name', '无明显叙事')}
强度: {narrative.get('dominant_narrative', {}).get('strength', 0)}

## 市场表现
- 美股能源: {performance.get('us_energy', {}).get('change_pct', 0)}%
- 中国煤炭: {performance.get('cn_coal', {}).get('change_pct', 0)}%
- WTI原油: {performance.get('wti', {}).get('change_pct', 0)}%
- 美元指数: {performance.get('dxy', {}).get('change_pct', 0)}%

## 资金流向
{fund_flow.get('interpretation', '暂无数据')}

## 机构观点
{chr(10).join([f"- {v['institution']}: {v['view']}" for v in inst_views[:3]])}

## 交易机会雷达
{chr(10).join([f"- {o['type']}: {o['theme']} (评分{o['score']}/100, 时间{o['time_horizon']})" for o in opportunities[:3]])}

## 报告结构要求

### 一、本周核心叙事回顾
分析本周主导叙事如何演变，有哪些关键转折事件。

### 二、市场全景
各主要资产表现回顾，相关性分析。

### 三、资金流向解读
北向资金、ETF资金流向分析，机构仓位变化。

### 四、机构观点汇总
提炼主流投行/机构的核心观点分歧。

### 五、交易机会雷达
对雷达中的机会进行深入分析，给出风险提示。

### 六、下周展望
基于叙事跟踪和传导分析，预判下周走势。

请用专业、简洁的语言撰写，适合机构投资者阅读。
"""
    
    def _save_report(self, week_range: str, content: str, data: Dict):
        """保存报告"""
        try:
            sql = """
                INSERT INTO reports (report_type, title, content_md, data_json, generated_at)
                VALUES (%s, %s, %s, %s, %s)
            """
            title = f"EnergyPulse周报 ({week_range})"
            self.db.execute(sql, (
                "weekly_enhanced",
                title,
                content,
                json.dumps(data),
                datetime.utcnow()
            ))
            logger.info(f"周报已保存: {title}")
        except Exception as e:
            logger.error(f"保存周报失败: {e}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    reporter = EnhancedWeeklyReporter()
    report = reporter.generate()
    
    print(f"\n周报生成完成: {report['week']}")
    print(f"主导叙事: {report['data']['narrative'].get('dominant_narrative', {}).get('name')}")
    print(f"\n交易机会:")
    for opp in report['data']['opportunities'][:3]:
        print(f"  - {opp['theme']}: {opp['score']}分")
