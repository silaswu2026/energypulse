"""
日报生成器 V2 - 节假日处理 + 详细新闻清单 + 回测数据记录
"""

import json
import logging
from datetime import date, datetime, timedelta
from database import get_db
from ai.llm_client import LLMClient
import sys
sys.path.insert(0, "/app")
from skills.mx_adapter import collect_cn_energy_stocks
from utils.holiday import is_cn_market_open, get_last_trading_day

logger = logging.getLogger("energypulse.daily")


class DailyReportGenerator:
    def __init__(self):
        self.db = get_db()
        self.llm = LLMClient()
        self.report_date = date.today()

    def generate(self):
        logger.info("开始生成日报: " + str(self.report_date))
        
        data = self._collect_data()
        agents = self._generate_agents(data)
        content_md = self._generate_content(data, agents)
        content_html = self._render_html(content_md, data, agents)
        
        # 记录回测数据快照
        backtest_data = self._build_backtest_snapshot(data, agents)
        
        report = {
            "report_type": "daily",
            "report_date": self.report_date,
            "title": "EnergyPulse 日报 - " + str(self.report_date),
            "content_md": content_md,
            "content_html": content_html,
            "ai_model": "deepseek-v3",
            "data_snapshot": json.dumps(backtest_data, default=str),
            "published": False,
            "created_at": datetime.utcnow().isoformat(),
        }
        
        self._save_report(report)
        logger.info("日报生成完成")
        return report

    def _collect_data(self):
        today = self.report_date.isoformat()
        yesterday = (self.report_date - timedelta(days=1)).isoformat()
        
        # A股：获取最近一个交易日数据（处理节假日）
        cn_is_open, cn_reason = is_cn_market_open(self.report_date)
        if cn_is_open:
            cn_query_date = today
            cn_data_note = None
        else:
            cn_query_date = get_last_trading_day(self.report_date).isoformat()
            cn_data_note = "A股休市 - " + cn_reason + "，显示最近交易日(" + cn_query_date + ")数据"
        
        data = {
            "date": today,
            "us_stocks": [],
            "cn_stocks": [],
            "cn_market_status": {"is_open": cn_is_open, "reason": cn_reason, "query_date": cn_query_date, "note": cn_data_note},
            "commodities": [],
            "macro": [],
            "news": [],
        }
        
        # 美股数据
        data["us_stocks"] = self.db.query(
            "SELECT * FROM stock_daily WHERE market = %s AND trade_date = %s",
            ["US", today]
        ) or self.db.query(
            "SELECT * FROM stock_daily WHERE market = %s AND trade_date = %s",
            ["US", yesterday]
        )
        
        # A股数据 - 使用最近交易日
        data["cn_stocks"] = self.db.query(
            "SELECT * FROM stock_daily WHERE market = %s AND trade_date = %s",
            ["CN", cn_query_date]
        )
        
        data["commodities"] = self.db.query(
            "SELECT * FROM commodity_daily WHERE trade_date = %s", [today]
        ) or self.db.query(
            "SELECT * FROM commodity_daily WHERE trade_date = %s", [yesterday]
        )
        
        data["macro"] = self.db.query(
            "SELECT * FROM macro_indicators WHERE time::date = %s", [today]
        )
        
        # 新闻数据 - 最近24小时
        data["news"] = self.db.query(
            "SELECT * FROM news_sentiment WHERE published_at::date >= %s ORDER BY published_at DESC LIMIT 20",
            [yesterday]
        )
        
        return data

    def _generate_agents(self, data):
        agents = []
        
        # US_Macro
        fed_rate = next((m for m in data["macro"] if m["series_id"] == "FEDFUNDS"), None)
        cpi = next((m for m in data["macro"] if m["series_id"] == "CPIAUCSL"), None)
        dgs10 = next((m for m in data["macro"] if m["series_id"] == "DGS10"), None)
        
        macro_text = ""
        if fed_rate:
            macro_text += "利率" + str(fed_rate["value"]) + "% "
        if cpi:
            macro_text += "CPI " + str(cpi["value"]) + " "
        if dgs10:
            macro_text += "10Y " + str(dgs10["value"]) + "%"
        
        agents.append({
            "agent": "US_Macro",
            "conclusion": macro_text if macro_text else "宏观数据采集中",
            "suggestion": "中性" if fed_rate and float(fed_rate["value"]) < 4 else "关注"
        })
        
        # US_Micro
        us_change = 0
        if data["us_stocks"]:
            changes = [s.get("change_pct") or 0 for s in data["us_stocks"]]
            us_change = sum(changes) / len(changes) if changes else 0
        
        xle = next((s for s in data["us_stocks"] if s["symbol"] == "XLE"), None)
        xle_price = xle["close"] if xle else "N/A"
        
        agents.append({
            "agent": "US_Micro",
            "conclusion": "XLE=" + str(round(xle_price, 2)) + ", 板块均涨跌" + str(round(us_change, 2)) + "%" if xle else "美股数据采集中",
            "suggestion": "看多" if us_change > 0.5 else "看空" if us_change < -0.5 else "中性"
        })
        
        # China_Macro
        agents.append({
            "agent": "China_Macro",
            "conclusion": "中国宏观政策观察中",
            "suggestion": "待更新"
        })
        
        # China_Micro - 显示休市信息
        cn_status = data.get("cn_market_status", {})
        cn_change = 0
        if data["cn_stocks"]:
            changes = [s.get("change_pct") or 0 for s in data["cn_stocks"]]
            cn_change = sum(changes) / len(changes) if changes else 0
        
        if not cn_status.get("is_open"):
            cn_conclusion = "A股" + cn_status.get("reason", "休市") + "，显示" + cn_status.get("query_date", "") + "数据"
        else:
            cn_conclusion = "A股煤炭板块均涨跌" + str(round(cn_change, 2)) + "%"
        
        agents.append({
            "agent": "China_Micro",
            "conclusion": cn_conclusion,
            "suggestion": "看多" if cn_change > 1 else "看空" if cn_change < -1 else "中性"
        })
        
        # Commodity
        brent = next((c for c in data["commodities"] if c["commodity_id"] == "BRENT"), None)
        gold = next((c for c in data["commodities"] if c["commodity_id"] == "COMEX_GOLD"), None)
        vix = next((c for c in data["commodities"] if c["commodity_id"] == "VIX"), None)
        
        comm_text = ""
        if brent:
            comm_text += "原油$" + str(brent["value"]) + " "
        if gold:
            comm_text += "黄金$" + str(gold["value"]) + " "
        if vix:
            comm_text += "VIX=" + str(vix["value"])
        
        vix_val = float(vix["value"]) if vix else 20
        agents.append({
            "agent": "Commodity",
            "conclusion": comm_text if comm_text else "数据采集中",
            "suggestion": "关注" if vix_val > 20 else "中性"
        })
        
        # Sentiment
        news_count = len(data["news"])
        sentiment_keywords = {"上涨": "正面", "增长": "正面", "突破": "正面", "下跌": "负面", "暴跌": "负面", "风险": "负面"}
        sentiment_score = 0
        for n in data["news"]:
            title = (n.get("title") or "").lower()
            for k, v in sentiment_keywords.items():
                if k in title:
                    sentiment_score += 1 if v == "正面" else -1
        
        sentiment = "正面" if sentiment_score > 0 else "负面" if sentiment_score < 0 else "中性"
        agents.append({
            "agent": "Sentiment",
            "conclusion": "采集" + str(news_count) + "条新闻，情绪" + sentiment,
            "suggestion": "看多" if sentiment == "正面" else "看空" if sentiment == "负面" else "中性"
        })
        
        return agents

    def _build_backtest_snapshot(self, data, agents):
        """构建回测数据快照 - 记录关键指标用于后续验证"""
        snapshot = {
            "report_date": str(self.report_date),
            "us_market": {
                "xle_price": None,
                "xle_change_pct": None,
                "btu_change_pct": None,
                "sector_avg_change": None,
            },
            "cn_market": {
                "is_open": data.get("cn_market_status", {}).get("is_open"),
                "query_date": data.get("cn_market_status", {}).get("query_date"),
                "coal_sector_avg_change": None,
            },
            "commodity": {
                "brent_price": None,
                "gold_price": None,
                "vix": None,
            },
            "macro": {
                "fed_rate": None,
                "cpi": None,
                "dgs10": None,
            },
            "news_count": len(data.get("news", [])),
            "agents": agents,
            "direction": self._calculate_direction(agents),
        }
        
        # 填充数据
        for s in data.get("us_stocks", []):
            if s["symbol"] == "XLE":
                snapshot["us_market"]["xle_price"] = s.get("close")
                snapshot["us_market"]["xle_change_pct"] = s.get("change_pct")
            if s["symbol"] == "BTU":
                snapshot["us_market"]["btu_change_pct"] = s.get("change_pct")
        
        if data.get("us_stocks"):
            changes = [s.get("change_pct") or 0 for s in data["us_stocks"]]
            snapshot["us_market"]["sector_avg_change"] = round(sum(changes)/len(changes), 2) if changes else None
        
        if data.get("cn_stocks"):
            changes = [s.get("change_pct") or 0 for s in data["cn_stocks"]]
            snapshot["cn_market"]["coal_sector_avg_change"] = round(sum(changes)/len(changes), 2) if changes else None
        
        for c in data.get("commodities", []):
            if c["commodity_id"] == "BRENT":
                snapshot["commodity"]["brent_price"] = c.get("value")
            if c["commodity_id"] == "COMEX_GOLD":
                snapshot["commodity"]["gold_price"] = c.get("value")
            if c["commodity_id"] == "VIX":
                snapshot["commodity"]["vix"] = c.get("value")
        
        for m in data.get("macro", []):
            if m["series_id"] == "FEDFUNDS":
                snapshot["macro"]["fed_rate"] = m.get("value")
            if m["series_id"] == "CPIAUCSL":
                snapshot["macro"]["cpi"] = m.get("value")
            if m["series_id"] == "DGS10":
                snapshot["macro"]["dgs10"] = m.get("value")
        
        return snapshot

    def _calculate_direction(self, agents):
        bullish = sum(1 for a in agents if a["suggestion"] == "看多")
        bearish = sum(1 for a in agents if a["suggestion"] == "看空")
        if bullish > bearish:
            return "看多"
        elif bearish > bullish:
            return "看空"
        return "中性"

    def _generate_content(self, data, agents):
        prompt = self._build_prompt(data, agents)
        
        system = """你是EnergyPulse能源宏观分析系统的首席分析师。生成专业的能源市场日报。
要求：
1. 使用中文撰写，专业严谨
2. 包含市场概况、关键数据、新闻清单与解读、后市展望
3. 新闻部分需要列出具体新闻标题清单
4. 最后包含6个Agent分析汇总表格
5. 给出明确的方向性判断"""

        try:
            content = self.llm.call("report", prompt, system=system, max_tokens=4000)
            return content
        except Exception as e:
            logger.error("AI生成失败: " + str(e))
            return self._fallback_report(data, agents)

    def _build_prompt(self, data, agents):
        lines = ["# 能源市场数据 (" + data["date"] + ")", ""]
        
        # 美股
        lines.append("## 美股能源板块")
        for s in data["us_stocks"]:
            change = s.get("change_pct") or 0
            lines.append("- " + s["symbol"] + ": $" + str(s["close"]) + " (" + str(round(change, 2)) + "%)")
        lines.append("")
        
        # A股状态
        cn_status = data.get("cn_market_status", {})
        lines.append("## A股市场")
        if cn_status.get("note"):
            lines.append("**" + cn_status["note"] + "**")
        for s in data["cn_stocks"]:
            change = s.get("change_pct") or 0
            lines.append("- " + s["symbol"] + ": " + str(s["close"]) + " (" + str(round(change, 2)) + "%)")
        lines.append("")
        
        # 宏观
        lines.append("## 宏观指标")
        for m in data["macro"]:
            lines.append("- " + m["series_name"] + ": " + str(m["value"]) + " " + m["unit"])
        lines.append("")
        
        # 商品
        lines.append("## 大宗商品")
        for c in data["commodities"]:
            lines.append("- " + c["commodity_id"] + ": " + str(c["value"]))
        lines.append("")
        
        # 新闻清单 - 详细列出
        lines.append("## 新闻清单（" + str(len(data["news"])) + "条）")
        for i, n in enumerate(data["news"][:15], 1):
            lines.append(str(i) + ". [" + n["source"] + "] " + n["title"])
        lines.append("")
        
        # Agent汇总
        lines.append("## 6个Agent分析汇总")
        lines.append("| Agent | 分析结论 | 建议 |")
        lines.append("|-------|----------|------|")
        for a in agents:
            lines.append("| " + a["agent"] + " | " + a["conclusion"] + " | " + a["suggestion"] + " |")
        lines.append("")
        
        direction = self._calculate_direction(agents)
        lines.append("**综合方向判断: " + direction + "**")
        lines.append("")
        
        lines.append("请基于以上数据生成完整日报。要求：")
        lines.append("1. 市场概况总结整体走势")
        lines.append("2. 关键数据突出重要变化")
        lines.append("3. 新闻解读部分要对上述新闻清单进行分析和归类")
        lines.append("4. 后市展望给出短期和中期判断")
        
        return "\n".join(lines)

    def _fallback_report(self, data, agents):
        lines = ["# EnergyPulse 日报 - " + data["date"], ""]
        lines.append("## 市场概况")
        cn_status = data.get("cn_market_status", {})
        if cn_status.get("note"):
            lines.append(cn_status["note"])
        lines.append("")
        lines.append("## 6个Agent分析汇总")
        lines.append("| Agent | 分析结论 | 建议 |")
        lines.append("|-------|----------|------|")
        for a in agents:
            lines.append("| " + a["agent"] + " | " + a["conclusion"] + " | " + a["suggestion"] + " |")
        return "\n".join(lines)

    def _render_html(self, markdown, data, agents):
        import markdown as md
        
        direction = self._calculate_direction(agents)
        direction_map = {
            "看多": ("bullish", "▲"),
            "看空": ("bearish", "▼"),
            "中性": ("neutral", "→")
        }
        direction_class, direction_icon = direction_map.get(direction, ("neutral", "→"))
        
        cards_html = self._generate_cards_html(data)
        content_html = md.markdown(markdown, extensions=["tables"])
        
        # A股休市提示
        cn_status = data.get("cn_market_status", {})
        market_notice = ""
        if not cn_status.get("is_open") and cn_status.get("note"):
            market_notice = '<div class="market-notice">' + cn_status["note"] + '</div>'
        
        date_str = self.report_date.strftime("%Y年%m月%d日")
        created_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
        
        html = '<!DOCTYPE html><html lang="zh-CN"><head><meta charset="utf-8">'
        html += '<title>EnergyPulse 能源市场日报</title>'
        html += '<style>' + self._get_css() + '</style></head><body>'
        html += '<div class="header"><div class="brand">EnergyPulse · 云晖未来 AI</div>'
        html += '<h1>能源市场日报</h1><div class="header-meta">' + date_str + ' | 首席分析师：EnergyPulse AI</div></div>'
        html += market_notice
        html += '<div class="direction-indicator"><div class="direction-icon ' + direction_class + '">' + direction_icon + '</div>'
        html += '<div class="direction-text"><div class="direction-label">今日风向</div>'
        html += '<div class="direction-value ' + direction_class + '">' + direction + '</div></div></div>'
        html += cards_html
        html += '<div class="content">' + content_html + '</div>'
        html += '<footer><span class="footer-brand">EnergyPulse</span> 全球能源宏观分析系统<br>'
        html += '生成时间：' + created_str + '<br>数据来源：FRED, FMP, Tavily, Tushare</footer>'
        html += '</body></html>'
        return html

    def _get_css(self):
        return """
        :root { --bg-primary: #0d1117; --bg-secondary: #161b22; --bg-card: #21262d; --bg-hover: #30363d;
                --text-primary: #c9d1d9; --text-secondary: #8b949e; --text-muted: #6e7681; --border: #30363d;
                --brand: #58a6ff; --brand-light: #79c0ff; --up: #3fb950; --down: #f85149; --neutral: #d29922; }
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "Noto Sans SC", sans-serif;
               background: var(--bg-primary); color: var(--text-primary); line-height: 1.6;
               max-width: 900px; margin: 0 auto; padding: 20px; }
        .header { text-align: center; padding: 30px 20px;
                  background: linear-gradient(135deg, var(--bg-secondary) 0%, var(--bg-card) 100%);
                  border-radius: 12px; margin-bottom: 24px; border: 1px solid var(--border); }
        .brand { font-size: 12px; color: var(--text-muted); letter-spacing: 2px; text-transform: uppercase; margin-bottom: 8px; }
        .header h1 { font-size: 28px; font-weight: 700; color: var(--text-primary); margin-bottom: 12px; border: none; }
        .header-meta { font-size: 14px; color: var(--text-secondary); }
        .market-notice { background: rgba(210, 153, 34, 0.1); border: 1px solid var(--neutral); border-radius: 8px;
                         padding: 12px 16px; margin: 16px 0; color: var(--neutral); font-size: 14px; }
        .direction-indicator { display: flex; align-items: center; justify-content: center; gap: 16px;
                               margin: 24px 0; padding: 20px; background: var(--bg-secondary);
                               border-radius: 12px; border: 2px solid var(--border); }
        .direction-icon { font-size: 48px; font-weight: bold; }
        .direction-icon.bullish { color: var(--up); } .direction-icon.bearish { color: var(--down); }
        .direction-icon.neutral { color: var(--neutral); }
        .direction-text { text-align: left; }
        .direction-label { font-size: 12px; color: var(--text-muted); text-transform: uppercase; letter-spacing: 1px; }
        .direction-value { font-size: 32px; font-weight: 700; }
        .direction-value.bullish { color: var(--up); } .direction-value.bearish { color: var(--down); }
        .direction-value.neutral { color: var(--neutral); }
        .cards-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 16px; margin: 24px 0; }
        .card { background: var(--bg-secondary); border: 1px solid var(--border); border-radius: 8px;
                padding: 16px; transition: transform 0.2s, border-color 0.2s; }
        .card:hover { transform: translateY(-2px); border-color: var(--brand); }
        .card-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px; }
        .card-symbol { font-size: 16px; font-weight: 600; color: var(--text-primary); }
        .card-badge { font-size: 11px; padding: 2px 8px; border-radius: 4px; background: var(--bg-hover); color: var(--text-secondary); }
        .card-price { font-size: 24px; font-weight: 700; color: var(--text-primary); margin-bottom: 4px; }
        .card-change { font-size: 14px; font-weight: 600; }
        .up { color: var(--up); } .down { color: var(--down); } .neutral { color: var(--neutral); }
        .content { background: var(--bg-secondary); border: 1px solid var(--border); border-radius: 12px; padding: 24px; margin-top: 24px; }
        h2 { font-size: 20px; font-weight: 600; color: var(--text-primary); margin: 32px 0 16px 0;
             padding-bottom: 8px; border-bottom: 2px solid var(--brand); }
        h2:first-child { margin-top: 0; }
        h3 { font-size: 16px; font-weight: 600; color: var(--brand-light); margin: 24px 0 12px 0; }
        p { margin-bottom: 12px; color: var(--text-primary); }
        ul, ol { margin: 12px 0; padding-left: 24px; }
        li { margin-bottom: 8px; }
        strong { color: var(--brand-light); }
        table { width: 100%; border-collapse: collapse; margin: 16px 0; font-size: 14px; }
        th { background: var(--bg-card); color: var(--brand-light); font-weight: 600; text-align: left;
             padding: 12px; border-bottom: 2px solid var(--brand); }
        td { padding: 12px; border-bottom: 1px solid var(--border); color: var(--text-primary); }
        tr:hover { background: var(--bg-hover); } tr:nth-child(even) { background: var(--bg-card); }
        footer { margin-top: 40px; padding: 20px; text-align: center; border-top: 1px solid var(--border);
                 color: var(--text-muted); font-size: 12px; }
        .footer-brand { font-weight: 600; color: var(--brand); }
        """

    def _generate_cards_html(self, data):
        cards = []
        
        xle = next((s for s in data.get("us_stocks", []) if s["symbol"] == "XLE"), None)
        if xle:
            change = xle.get("change_pct") or 0
            change_class = "up" if change >= 0 else "down"
            change_sign = "+" if change >= 0 else ""
            cards.append('<div class="card"><div class="card-header"><span class="card-symbol">XLE</span>'
                        '<span class="card-badge">能源ETF</span></div><div class="card-price">$' + str(round(xle["close"], 2)) + '</div>'
                        '<div class="card-change ' + change_class + '">' + change_sign + str(round(change, 2)) + '%</div></div>')
        
        btu = next((s for s in data.get("us_stocks", []) if s["symbol"] == "BTU"), None)
        if btu:
            change = btu.get("change_pct") or 0
            change_class = "up" if change >= 0 else "down"
            change_sign = "+" if change >= 0 else ""
            cards.append('<div class="card"><div class="card-header"><span class="card-symbol">BTU</span>'
                        '<span class="card-badge">煤炭</span></div><div class="card-price">$' + str(round(btu["close"], 2)) + '</div>'
                        '<div class="card-change ' + change_class + '">' + change_sign + str(round(change, 2)) + '%</div></div>')
        
        brent = next((c for c in data.get("commodities", []) if c["commodity_id"] == "BRENT"), None)
        if brent:
            cards.append('<div class="card"><div class="card-header"><span class="card-symbol">原油</span>'
                        '<span class="card-badge">商品</span></div><div class="card-price">$' + str(round(brent["value"], 2)) + '</div>'
                        '<div class="card-change neutral">Brent</div></div>')
        
        vix = next((c for c in data.get("commodities", []) if c["commodity_id"] == "VIX"), None)
        if vix:
            cards.append('<div class="card"><div class="card-header"><span class="card-symbol">VIX</span>'
                        '<span class="card-badge">波动率</span></div><div class="card-price">' + str(round(vix["value"], 2)) + '</div>'
                        '<div class="card-change neutral">恐慌指数</div></div>')
        
        fed = next((m for m in data.get("macro", []) if m["series_id"] == "FEDFUNDS"), None)
        if fed:
            cards.append('<div class="card"><div class="card-header"><span class="card-symbol">利率</span>'
                        '<span class="card-badge">宏观</span></div><div class="card-price">' + str(fed["value"]) + '%</div>'
                        '<div class="card-change neutral">联邦基金</div></div>')
        
        if cards:
            return '<div class="cards-grid">' + "".join(cards) + '</div>'
        return ""

    def _save_report(self, report):
        sql = """
            INSERT INTO reports (report_type, report_date, title, content_md, content_html,
                                 ai_model, data_snapshot, published, created_at)
            VALUES (%(report_type)s, %(report_date)s, %(title)s, %(content_md)s, %(content_html)s,
                    %(ai_model)s, %(data_snapshot)s, %(published)s, %(created_at)s)
            ON CONFLICT (report_type, report_date)
            DO UPDATE SET title = EXCLUDED.title,
                          content_md = EXCLUDED.content_md,
                          content_html = EXCLUDED.content_html
        """
        self.db.execute(sql, report)


if __name__ == "__main__":
    gen = DailyReportGenerator()
    report = gen.generate()
    print("报告已生成: " + report.get("title"))
