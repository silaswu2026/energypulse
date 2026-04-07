"""
EnergyPulse 主调度器 (北京时区优化版)

目标: 每日北京时间09:00前发布报告
策略: 异步跨市场数据汇总 + 回测验证

时间线:
  06:00 - 采集美国数据 (美股收盘2小时后)
  07:30 - 采集供应链数据 (BDI/港口/电厂)
  08:00 - 采集中国数据 (A股开盘前)
  08:30 - 生成日报 (确保09:00前发布)
  16:00 - 采集A股收盘数据 (用于次日回测)
"""

import os
import sys
import logging
import signal
from datetime import datetime

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("/app/logs/app.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("energypulse.main")

# 确保模块路径
sys.path.insert(0, os.path.dirname(__file__))

from database import get_db


def run_collector(collector_class, source_id: str, **kwargs):
    """安全运行单个采集器，捕获所有异常"""
    try:
        collector = collector_class(source_id, **kwargs)
        collector.run()
    except Exception as e:
        logger.error(f"采集器 [{source_id}] 异常: {e}", exc_info=True)


# ── 定时任务函数 ──

def job_us_data_collection():
    """
    美国数据采集 (北京时间06:00)
    美股收盘时间: 美东16:00 = 北京04:00/05:00
    延迟1小时确保数据更新
    """
    logger.info("=== [06:00 Beijing] 开始采集美国数据 (T-1收盘) ===")
    
    try:
        from collectors.fred_macro import collect_fred
        from collectors.fmp_stocks import FMPStockCollector
        from collectors.eia_prices import collect_eia
        from collectors.fmp_commodities import FMPCommodityCollector
        
        # FRED宏观数据
        collect_fred()
        logger.info("✅ FRED宏观数据已采集")
        
        # 美股能源ETF
        fmp = FMPStockCollector()
        fmp.collect_etf("XLE")
        logger.info("✅ 美股能源ETF已采集")
        
        # EIA能源数据
        collect_eia()
        logger.info("✅ EIA能源数据已采集")
        
        # 大宗商品
        run_collector(FMPCommodityCollector, "commodities")
        logger.info("✅ 大宗商品数据已采集")
        
    except Exception as e:
        logger.error(f"美国数据采集失败: {e}", exc_info=True)


def job_supply_chain_collection():
    """
    供应链数据采集 (北京时间07:30)
    BDI、港口库存、电厂日耗
    """
    logger.info("=== [07:30 Beijing] 开始采集供应链数据 ===")
    
    try:
        from collectors.supply_chain.mx_supply_collector import MXSupplyChainCollector
        
        collector = MXSupplyChainCollector()
        results = collector.collect_all()
        
        for key, success in results.items():
            status = "✅" if success else "❌"
            logger.info(f"  {status} {key}")
            
    except Exception as e:
        logger.error(f"供应链数据采集失败: {e}", exc_info=True)


def job_cn_data_collection():
    """
    中国数据采集 (北京时间08:00)
    A股开盘前采集T-1数据
    """
    logger.info("=== [08:00 Beijing] 开始采集中国数据 ===")
    
    try:
        from collectors.tushare_stocks import collect_cn_coal_stocks
        
        # A股煤炭股
        collect_cn_coal_stocks()
        logger.info("✅ 中国煤炭股数据已采集")
        
    except Exception as e:
        logger.error(f"中国数据采集失败: {e}", exc_info=True)


def job_generate_daily_report():
    """
    生成日报 (北京时间08:30)
    综合T-1国内外数据生成今日策略
    """
    logger.info("=== [08:30 Beijing] 开始生成日报 ===")
    
    try:
        from reporters.daily import DailyReportGenerator
        
        gen = DailyReportGenerator()
        report = gen.generate()
        
        logger.info(f"✅ 日报已生成: {report.get('title', 'N/A')}")
        logger.info(f"   报告将用于今日09:30开盘决策")
        
    except Exception as e:
        logger.error(f"日报生成失败: {e}", exc_info=True)


def job_cn_market_close():
    """
    A股收盘数据采集 (北京时间16:00)
    用于回测验证
    """
    logger.info("=== [16:00 Beijing] 开始采集A股收盘数据 ===")
    
    try:
        from collectors.tushare_stocks import TushareCNStockCollector
        run_collector(TushareCNStockCollector, "cn_stocks")
        logger.info("✅ A股收盘数据已采集")
        
    except Exception as e:
        logger.error(f"A股收盘数据采集失败: {e}", exc_info=True)


def job_news_collection():
    """新闻舆情采集 (每4小时)"""
    logger.info("=== 开始新闻采集 ===")
    
    try:
        from collectors.news_tavily import TavilyNewsCollector
        from collectors.news_enhanced import TieredNewsCollector
        
        # 基础新闻
        run_collector(TavilyNewsCollector, "news_tavily")
        
        # 分层高冲击新闻
        collector = TieredNewsCollector()
        results = collector.collect_all()
        logger.info(f"  Tier1高冲击新闻: {len(results.get('tier1', []))} 条")
        
    except Exception as e:
        logger.error(f"新闻采集失败: {e}", exc_info=True)


def job_technical_calc():
    """技术指标计算 (北京时间02:00)"""
    logger.info("=== 开始技术指标计算 ===")
    
    try:
        from processors.technical import TechnicalProcessor
        proc = TechnicalProcessor()
        proc.calculate_all()
        logger.info("✅ 技术指标计算完成")
        
    except Exception as e:
        logger.error(f"技术指标计算失败: {e}", exc_info=True)


def job_weather_collection():
    """天气HDD/CDD采集 (北京时间00:30)"""
    logger.info("=== 开始天气数据采集 ===")
    
    try:
        from collectors.weather_hddcdd import WeatherHDDCDDCollector
        run_collector(WeatherHDDCDDCollector, "weather")
        logger.info("✅ 天气数据已采集")
        
    except Exception as e:
        logger.error(f"天气数据采集失败: {e}", exc_info=True)


def job_weekly_report():
    """生成周报 (每周日08:00)"""
    logger.info("=== 开始生成周报 ===")
    
    try:
        from reporters.enhanced.weekly_enhanced import EnhancedWeeklyReporter
        
        reporter = EnhancedWeeklyReporter()
        report = reporter.generate()
        
        logger.info(f"✅ 周报已生成")
        
    except Exception as e:
        logger.error(f"周报生成失败: {e}", exc_info=True)


def job_monthly_report():
    """生成月报 (每月1日10:00)"""
    logger.info("=== 开始生成月报 ===")
    
    try:
        from reporters.monthly import MonthlyReportGenerator
        
        gen = MonthlyReportGenerator()
        report = gen.generate()
        
        logger.info(f"✅ 月报已生成")
        
    except Exception as e:
        logger.error(f"月报生成失败: {e}", exc_info=True)


def job_backtest_record():
    """
    回测数据记录 (每日16:30)
    对比T日实际走势 vs T日早预测
    """
    logger.info("=== [16:30 Beijing] 开始回测数据记录 ===")
    
    try:
        # 获取T日早的预测信号
        db = get_db()
        
        # 获取T日A股实际涨跌幅
        sql_actual = """
            SELECT symbol, change_pct
            FROM stock_daily
            WHERE trade_date = CURRENT_DATE
            AND symbol IN ('601088.SH', '601225.SH')
        """
        actual_results = db.query(sql_actual)
        
        # 获取T日早的AI预测
        sql_predict = """
            SELECT data_json->>'prediction' as prediction,
                   data_json->>'confidence' as confidence
            FROM reports
            WHERE report_type = 'daily'
            AND generated_at::date = CURRENT_DATE
            ORDER BY generated_at DESC
            LIMIT 1
        """
        prediction = db.query_one(sql_predict)
        
        if prediction and actual_results:
            # 记录回测结果
            avg_return = sum(r['change_pct'] for r in actual_results) / len(actual_results)
            
            logger.info(f"回测记录:")
            logger.info(f"  预测: {prediction.get('prediction', 'N/A')}")
            logger.info(f"  实际收益: {avg_return:.2f}%")
            logger.info(f"  记录待保存到backtest_results表")
        
    except Exception as e:
        logger.error(f"回测记录失败: {e}", exc_info=True)


def job_health_check():
    """系统健康检查 (每5分钟)"""
    try:
        db = get_db()
        db.query_one("SELECT 1")
    except Exception as e:
        logger.error(f"健康检查失败: {e}")


def main():
    logger.info("=" * 60)
    logger.info("EnergyPulse 系统启动 (北京时区优化版)")
    logger.info(f"时间: {datetime.now().isoformat()}")
    logger.info(f"PID: {os.getpid()}")
    logger.info("=" * 60)

    # 验证数据库连接
    try:
        db = get_db()
        db.query_one("SELECT 1")
        logger.info("数据库连接正常")
    except Exception as e:
        logger.error(f"数据库连接失败: {e}")
        sys.exit(1)

    # 创建调度器（北京时区）
    scheduler = BlockingScheduler(timezone="Asia/Shanghai")

    # ── 核心数据流 (06:00-08:30) ──
    
    # 06:00 美国数据采集 (美股收盘后2小时)
    scheduler.add_job(job_us_data_collection,
                      CronTrigger(hour=6, minute=0),
                      id="us_data", name="[核心]美国数据采集")

    # 07:30 供应链数据采集
    scheduler.add_job(job_supply_chain_collection,
                      CronTrigger(hour=7, minute=30),
                      id="supply_chain", name="[核心]供应链数据采集")

    # 08:00 中国数据采集
    scheduler.add_job(job_cn_data_collection,
                      CronTrigger(hour=8, minute=0),
                      id="cn_data", name="[核心]中国数据采集")

    # 08:30 生成日报 (09:00前发布)
    scheduler.add_job(job_generate_daily_report,
                      CronTrigger(hour=8, minute=30),
                      id="daily_report", name="[核心]生成日报")

    # 16:00 A股收盘数据采集
    scheduler.add_job(job_cn_market_close,
                      CronTrigger(hour=16, minute=0),
                      id="cn_close", name="A股收盘数据采集")

    # 16:30 回测数据记录
    scheduler.add_job(job_backtest_record,
                      CronTrigger(hour=16, minute=30),
                      id="backtest", name="回测数据记录")

    # ── 辅助任务 ──

    # 每4小时新闻采集
    scheduler.add_job(job_news_collection,
                      CronTrigger(hour="2,6,10,14,18,22", minute=0),
                      id="news", name="新闻采集")

    # 00:30 天气数据
    scheduler.add_job(job_weather_collection,
                      CronTrigger(hour=0, minute=30),
                      id="weather", name="天气数据")

    # 02:00 技术指标计算
    scheduler.add_job(job_technical_calc,
                      CronTrigger(hour=2, minute=0),
                      id="technical", name="技术指标计算")

    # 每5分钟健康检查
    scheduler.add_job(job_health_check,
                      CronTrigger(minute="*/5"),
                      id="health", name="健康检查")

    # ── 周月报 ──

    # 周日08:00 周报
    scheduler.add_job(job_weekly_report,
                      CronTrigger(day_of_week="sun", hour=8, minute=0),
                      id="weekly", name="生成周报")

    # 每月1日10:00 月报
    scheduler.add_job(job_monthly_report,
                      CronTrigger(day=1, hour=10, minute=0),
                      id="monthly", name="生成月报")

    # 优雅退出
    def shutdown(signum, frame):
        logger.info("收到退出信号，正在关闭...")
        scheduler.shutdown(wait=False)
        get_db().close()
        sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    # 启动调度器
    logger.info(f"已注册 {len(scheduler.get_jobs())} 个定时任务")
    logger.info("核心任务时间线:")
    logger.info("  06:00 - 美国数据 (T-1收盘)")
    logger.info("  07:30 - 供应链数据 (BDI/港口/电厂)")
    logger.info("  08:00 - 中国数据 (T-1 A股)")
    logger.info("  08:30 - 生成日报 (09:00前发布)")
    logger.info("  16:00 - A股收盘 (用于回测)")
    
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("系统已停止")


if __name__ == "__main__":
    main()
