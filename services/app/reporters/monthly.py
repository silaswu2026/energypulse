"""
月报生成器
"""

import json
import logging
from datetime import date, datetime, timedelta
from calendar import monthrange
from database import get_db
from ai.llm_client import LLMClient

logger = logging.getLogger("energypulse.monthly")


class MonthlyReportGenerator:
    def __init__(self, year=None, month=None):
        self.db = get_db()
        self.llm = LLMClient()
        
        today = date.today()
        if year is None or month is None:
            first_day = today.replace(day=1)
            last_day = first_day - timedelta(days=1)
            self.year = last_day.year
            self.month = last_day.month
        else:
            self.year = year
            self.month = month
        
        self.start_date = date(self.year, self.month, 1)
        last = monthrange(self.year, self.month)[1]
        self.end_date = date(self.year, self.month, last)

    def generate(self):
        logger.info(f"开始生成月报: {self.year}年{self.month}月")
        
        data = self._collect_monthly_data()
        content_md = self._generate_content(data)
        content_html = self._render_html(content_md, data)
        
        report = {
            "report_type": "monthly",
            "report_date": self.end_date,
            "title": f"EnergyPulse 月报 - {self.year}年{self.month}月",
            "content_md": content_md,
            "content_html": content_html,
            "ai_model": "deepseek-v3",
            "data_snapshot": json.dumps(data, default=str),
            "published": False,
            "created_at": datetime.utcnow().isoformat(),
        }
        
        self._save_report(report)
        logger.info(f"月报生成完成")
        return report

    def _collect_monthly_data(self):
        start_str = self.start_date.isoformat()
        end_str = self.end_date.isoformat()
        
        data = {
            "period": f"{self.year}年{self.month}月",
            "us_stocks": [],
            "commodities": [],
        }
        
        data["us_stocks"] = self.db.query(
            """SELECT symbol, 
                      ((MAX(close) - MIN(close)) / MIN(close) * 100) as swing_pct
               FROM stock_daily 
               WHERE market = %s AND trade_date BETWEEN %s AND %s
               GROUP BY symbol""",
            ["US", start_str, end_str]
        )
        
        data["commodities"] = self.db.query(
            """SELECT commodity_id, AVG(value) as avg_val
               FROM commodity_daily 
               WHERE trade_date BETWEEN %s AND %s
               GROUP BY commodity_id""",
            [start_str, end_str]
        )
        
        return data

    def _generate_content(self, data):
        lines = [f"# EnergyPulse 月报 ({data["period"]})", ""]
        lines.append("## 本月市场回顾")
        lines.append("")
        lines.append("### 美股能源板块")
        
        for s in data["us_stocks"]:
            symbol = s["symbol"]
            swing = s.get("swing_pct", 0)
            lines.append(f"- {symbol}: 月振幅{swing:.2f}%")
        
        lines.append("")
        lines.append("### 大宗商品")
        for c in data["commodities"]:
            cid = c["commodity_id"]
            avg = c.get("avg_val", 0)
            lines.append(f"- {cid}: 月均价{avg:.2f}")
        
        lines.append("")
        lines.append("## 下月展望")
        lines.append("基于本月数据，建议关注能源板块长期趋势。")
        
        return chr(10).join(lines)

    def _render_html(self, markdown, data):
        import markdown as md
        content_html = md.markdown(markdown, extensions=["tables"])
        
        return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8">
<title>EnergyPulse 月报</title>
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
    gen = MonthlyReportGenerator()
    report = gen.generate()
    print(f"月报已生成")
