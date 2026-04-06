"""
周报生成器
"""

import json
import logging
from datetime import date, datetime, timedelta
from database import get_db
from ai.llm_client import LLMClient

logger = logging.getLogger("energypulse.weekly")


class WeeklyReportGenerator:
    def __init__(self, target_date=None):
        self.db = get_db()
        self.llm = LLMClient()
        
        if target_date is None:
            today = date.today()
            self.end_date = today - timedelta(days=today.weekday() + 1)
        else:
            self.end_date = target_date
        
        self.start_date = self.end_date - timedelta(days=6)
        self.report_date = self.end_date

    def generate(self):
        logger.info(f"开始生成周报: {self.start_date} ~ {self.end_date}")
        
        data = self._collect_weekly_data()
        content_md = self._generate_content(data)
        content_html = self._render_html(content_md, data)
        
        report = {
            "report_type": "weekly",
            "report_date": self.report_date,
            "title": f"EnergyPulse 周报 - {self.start_date} 至 {self.end_date}",
            "content_md": content_md,
            "content_html": content_html,
            "ai_model": "deepseek-v3",
            "data_snapshot": json.dumps(data, default=str),
            "published": False,
            "created_at": datetime.utcnow().isoformat(),
        }
        
        self._save_report(report)
        logger.info(f"周报生成完成")
        return report

    def _collect_weekly_data(self):
        start_str = self.start_date.isoformat()
        end_str = self.end_date.isoformat()
        
        data = {
            "period": f"{self.start_date} ~ {self.end_date}",
            "us_stocks": [],
            "commodities": [],
        }
        
        # 美股周度数据
        data["us_stocks"] = self.db.query(
            """SELECT symbol, AVG(close) as avg_close, 
                      ((MAX(close) - MIN(close)) / MIN(close) * 100) as swing_pct
               FROM stock_daily 
               WHERE market = %s AND trade_date BETWEEN %s AND %s
               GROUP BY symbol""",
            ["US", start_str, end_str]
        )
        
        # 商品数据
        data["commodities"] = self.db.query(
            "SELECT * FROM commodity_daily WHERE trade_date = %s",
            [end_str]
        )
        
        return data

    def _generate_content(self, data):
        # 构建简单周报内容
        lines = [f"# EnergyPulse 周报 ({data["period"]})", ""]
        lines.append("## 本周市场回顾")
        lines.append("")
        lines.append("### 美股能源板块")
        
        for s in data["us_stocks"]:
            symbol = s["symbol"]
            avg = s.get("avg_close", 0)
            swing = s.get("swing_pct", 0)
            lines.append(f"- {symbol}: 均价{avg:.2f}, 周振幅{swing:.2f}%")
        
        lines.append("")
        lines.append("### 大宗商品")
        for c in data["commodities"]:
            cid = c["commodity_id"]
            val = c["value"]
            chg = c.get("change_pct", 0)
            lines.append(f"- {cid}: {val} ({chg}%)")
        
        lines.append("")
        lines.append("## 下周展望")
        lines.append("基于本周数据，建议关注能源板块走势和宏观政策变化。")
        
        return chr(10).join(lines)

    def _render_html(self, markdown, data):
        import markdown as md
        content_html = md.markdown(markdown, extensions=["tables"])
        
        return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8">
<title>EnergyPulse 周报</title>
<style>
body {{ font-family: -apple-system, sans-serif; max-width: 900px; margin: 0 auto; 
       padding: 20px; background: #0d1117; color: #c9d1d9; }}
h1 {{ color: #58a6ff; border-bottom: 2px solid #58a6ff; }}
h2 {{ color: #79c0ff; margin-top: 30px; }}
</style></head>
<body>
{content_html}
</body></html>"""

    def _save_report(self, report):
        sql = """
            INSERT INTO reports (report_type, report_date, title, content_md, content_html,
                                 ai_model, data_snapshot, published, created_at)
            VALUES (%(report_type)s, %(report_date)s, %(title)s, %(content_md)s, %(content_html)s,
                    %(ai_model)s, %(data_snapshot)s, %(published)s, %(created_at)s)
            ON CONFLICT (report_type, report_date) DO UPDATE SET
                title = EXCLUDED.title, content_md = EXCLUDED.content_md, content_html = EXCLUDED.content_html
        """
        self.db.execute(sql, report)


if __name__ == "__main__":
    gen = WeeklyReportGenerator()
    report = gen.generate()
    print(f"周报已生成")
