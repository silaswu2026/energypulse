"""
EnergyPulse Web服务器
"""

import os
import psycopg2
import psycopg2.extras
from flask import Flask
from contextlib import contextmanager

app = Flask(__name__)
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://epuser:changeme@postgres:5432/energypulse")

@contextmanager
def get_db():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def query(sql, params=None):
    with get_db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            return [dict(row) for row in cur.fetchall()]

def query_one(sql, params=None):
    rows = query(sql, params)
    return rows[0] if rows else None

@app.route("/")
def home():
    daily = query_one("SELECT * FROM reports WHERE report_type = %s ORDER BY report_date DESC LIMIT 1", ["daily"])
    weekly = query_one("SELECT * FROM reports WHERE report_type = %s ORDER BY report_date DESC LIMIT 1", ["weekly"])
    monthly = query_one("SELECT * FROM reports WHERE report_type = %s ORDER BY report_date DESC LIMIT 1", ["monthly"])
    
    daily_title = daily["title"] if daily else "暂无日报"
    weekly_title = weekly["title"] if weekly else "暂无周报"
    monthly_title = monthly["title"] if monthly else "暂无月报"
    
    return f"""
    <h1>EnergyPulse 能源宏观分析</h1>
    <ul>
        <li><a href="/report/daily/latest">{daily_title}</a></li>
        <li><a href="/report/weekly/latest">{weekly_title}</a></li>
        <li><a href="/report/monthly/latest">{monthly_title}</a></li>
    </ul>
    <p><a href="/reports">全部报告</a></p>
    """

@app.route("/reports")
def reports():
    all_reports = query("SELECT report_type, report_date, title FROM reports ORDER BY report_date DESC LIMIT 50")
    html = "<h1>报告列表</h1><ul>"
    for r in all_reports:
        url = f"/report/{r["report_type"]}/{r["report_date"]}"
        html += f"<li><a href={url}>{r["title"]}</a></li>"
    html += "</ul>"
    return html

@app.route("/report/<rtype>/<rdate>")
def report(rtype, rdate):
    if rdate == "latest":
        report = query_one("SELECT * FROM reports WHERE report_type = %s ORDER BY report_date DESC LIMIT 1", [rtype])
    else:
        report = query_one("SELECT * FROM reports WHERE report_type = %s AND report_date = %s", [rtype, rdate])
    
    if not report:
        return "报告未找到", 404
    
    return report["content_html"] or "暂无内容"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=80)
