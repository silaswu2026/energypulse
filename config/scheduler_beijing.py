"""
北京时区定时任务配置

目标: 每日北京时间 09:00 前发布报告
策略: 异步跨市场数据汇总
"""

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import logging
import sys
import os

sys.path.insert(0, "/app")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("energypulse.scheduler")

# 设置北京时区
TIMEZONE = "Asia/Shanghai"


def init_beijing_scheduler():
    """初始化北京时区定时任务"""
    scheduler = BackgroundScheduler(timezone=TIMEZONE)
    
    # === 第一阶段: 06:00 采集美国数据 (美股已收盘2小时) ===
    scheduler.add_job(
        func=collect_us_data,
        trigger=CronTrigger(hour=6, minute=0),
        id="us_data_collection",
        name="美国数据采集 (美股收盘后)",
        replace_existing=True
    )
    
    # === 第二阶段: 07:30 采集供应链数据 (BDI/港口/电厂) ===
    scheduler.add_job(
        func=collect_supply_chain_data,
        trigger=CronTrigger(hour=7, minute=30),
        id="supply_chain_collection",
        name="供应链数据采集",
        replace_existing=True
    )
    
    # === 第三阶段: 08:00 采集中国市场数据 (开盘前) ===
    scheduler.add_job(
        func=collect_cn_data,
        trigger=CronTrigger(hour=8, minute=0),
        id="cn_data_collection",
        name="中国数据采集 (开盘前)",
        replace_existing=True
    )
    
    # === 第四阶段: 08:30 生成报告 (确保09:00前完成) ===
    scheduler.add_job(
        func=generate_daily_report,
        trigger=CronTrigger(hour=8, minute=30),
        id="daily_report_generation",
        name="生成日报 (09:00前发布)",
        replace_existing=True
    )
    
    # === 回测任务: 每周六 10:00 生成周回测报告 ===
    scheduler.add_job(
        func=generate_backtest_report,
        trigger=CronTrigger(day_of_week="sat", hour=10, minute=0),
        id="weekly_backtest",
        name="周回测报告",
        replace_existing=True
    )
    
    logger.info(f"北京时区定时任务已配置，当前时区: {TIMEZONE}")
    return scheduler


def collect_us_data():
    """采集美国数据 (T-1)"""
    logger.info("[06:00 Beijing] 开始采集美国数据...")
    try:
        from collectors.fred_macro import collect_fred
        from collectors.fmp_stocks import FMPStockCollector
        from collectors.eia_prices import collect_eia
        
        # 宏观数据
        collect_fred()
        logger.info("✅ FRED宏观数据已采集")
        
        # 股票数据
        fmp = FMPStockCollector()
        fmp.collect_etf("XLE")
        logger.info("✅ 美股能源ETF已采集")
        
        # EIA数据
        collect_eia()
        logger.info("✅ EIA能源数据已采集")
        
    except Exception as e:
        logger.error(f"美国数据采集失败: {e}")


def collect_supply_chain_data():
    """采集供应链数据"""
    logger.info("[07:30 Beijing] 开始采集供应链数据...")
    try:
        from collectors.supply_chain.mx_supply_collector import MXSupplyChainCollector
        
        collector = MXSupplyChainCollector()
        results = collector.collect_all()
        
        for key, success in results.items():
            status = "✅" if success else "❌"
            logger.info(f"  {status} {key}")
            
    except Exception as e:
        logger.error(f"供应链数据采集失败: {e}")


def collect_cn_data():
    """采集中国数据 (T-1收盘数据)"""
    logger.info("[08:00 Beijing] 开始采集中国数据...")
    try:
        from collectors.tushare_stocks import collect_cn_coal_stocks
        
        # A股煤炭股
        collect_cn_coal_stocks()
        logger.info("✅ 中国煤炭股数据已采集")
        
    except Exception as e:
        logger.error(f"中国数据采集失败: {e}")


def generate_daily_report():
    """生成日报"""
    logger.info("[08:30 Beijing] 开始生成日报...")
    try:
        from reporters.daily import DailyReporter
        
        reporter = DailyReporter()
        report = reporter.generate()
        
        logger.info(f"✅ 日报已生成: {report.get(title, Unknown)}")
        logger.info(f"   报告将用于今日09:30开盘决策")
        
    except Exception as e:
        logger.error(f"日报生成失败: {e}")


def generate_backtest_report():
    """生成回测报告"""
    logger.info("[周六 10:00] 开始生成回测报告...")
    try:
        # 对比T-1国外数据预测 vs T日中国实际走势
        logger.info("回测逻辑: 验证前一日国外信号对A股的预测准确性")
        pass
    except Exception as e:
        logger.error(f"回测报告生成失败: {e}")


if __name__ == "__main__":
    scheduler = init_beijing_scheduler()
    scheduler.start()
    logger.info("北京时区调度器已启动，等待任务执行...")
    
    # 保持运行
    try:
        while True:
            import time
            time.sleep(60)
    except KeyboardInterrupt:
        scheduler.shutdown()
        logger.info("调度器已停止")
