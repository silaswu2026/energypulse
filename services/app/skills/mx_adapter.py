"""
妙想Skills适配器 - 将技能输出转换为EnergyPulse数据格式
"""

import os
import sys
import json
import logging
import subprocess
import glob
from datetime import date

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("energypulse.mx_adapter")

# 确保API Key
os.environ.setdefault("MX_APIKEY", "mkt_oto52LfYMH1rSIO6BLUgp5JbuwdYdfFsKNsfqQjeMKk")

SKILLS_BASE = "/app/skills"


def query_cn_stock(stock_name):
    """查询A股个股实时数据"""
    skill_dir = f"{SKILLS_BASE}/mx-data"
    
    try:
        # 运行技能脚本
        result = subprocess.run(
            ["python3", "mx_data.py", f"{stock_name}最新价 涨跌幅"],
            cwd=skill_dir,
            capture_output=True,
            text=True,
            timeout=30,
        )
        
        if result.returncode != 0:
            logger.error(f"mx-data执行失败: {result.stderr}")
            return None
        
        # 查找输出文件
        output_dir = "/root/.openclaw/workspace/mx_data/output"
        json_files = glob.glob(f"{output_dir}/mx_data_{stock_name}*_raw.json")
        
        if not json_files:
            logger.warning(f"未找到{stock_name}的输出文件")
            return None
        
        # 读取最新文件
        json_files.sort(key=lambda x: os.path.getmtime(x), reverse=True)
        with open(json_files[0], "r") as f:
            data = json.load(f)
        
        # 解析数据结构
        search_result = data.get("data", {}).get("data", {}).get("searchDataResultDTO", {})
        tables = search_result.get("dataTableDTOList", [])
        
        stock_data = {
            "symbol": stock_name,
            "market": "CN",
            "trade_date": date.today().isoformat(),
            "close": None,
            "change_pct": None,
            "source": "mx-data",
        }
        
        for table in tables:
            title = table.get("title", "")
            raw_table = table.get("rawTable", {})
            
            # 跳过港股数据(标题含.HK)
            if ".HK" in title:
                continue
            
            # 提取收盘价 - 查找非headName的数字键
            for key, values in raw_table.items():
                if key != "headName" and values and isinstance(values, list):
                    try:
                        stock_data["close"] = float(values[0])
                        break
                    except (ValueError, IndexError):
                        pass
        
        return stock_data
        
    except Exception as e:
        logger.error(f"查询{stock_name}失败: {e}")
        return None


def collect_cn_energy_stocks():
    """采集A股能源板块数据"""
    logger.info("使用妙想Skills采集A股能源数据...")
    
    energy_stocks = [
        "中国神华", "陕西煤业", "兖矿能源", "中煤能源", "山西焦煤",
        "潞安环能", "平煤股份", "晋控煤业", "华阳股份", "美锦能源",
    ]
    
    records = []
    for stock in energy_stocks:
        data = query_cn_stock(stock)
        if data and data.get("close"):
            records.append(data)
            logger.info(f"{stock}: {data[close]}")
        else:
            logger.warning(f"{stock}: 无数据")
    
    logger.info(f"采集完成: {len(records)}/{len(energy_stocks)}")
    return records


if __name__ == "__main__":
    # 测试
    result = query_cn_stock("宁德时代")
    print(f"Test result: {result}")


def search_mx_news(keywords=None, days=1):
    """
    使用mx-search搜索金融资讯
    返回EnergyPulse标准新闻格式
    """
    if keywords is None:
        keywords = ["能源", "煤炭", "石油", "新能源"]
    
    logger.info(f"使用mx-search搜索新闻: {keywords}")
    
    all_news = []
    
    for keyword in keywords:
        try:
            skill_dir = f"{SKILLS_BASE}/mx-search"
            query = f"{keyword} 近{days}天新闻"
            
            result = subprocess.run(
                ["python3", "mx_search.py", query],
                cwd=skill_dir,
                capture_output=True,
                text=True,
                timeout=30,
            )
            
            if result.returncode != 0:
                logger.error(f"mx-search执行失败: {result.stderr}")
                continue
            
            # 解析输出获取新闻列表
            # 尝试从JSON文件读取
            output_dir = "/root/.openclaw/workspace/mx_data/output"
            json_files = glob.glob(f"{output_dir}/mx_search_*.json")
            
            if json_files:
                json_files.sort(key=lambda x: os.path.getmtime(x), reverse=True)
                
                with open(json_files[0], "r", encoding="utf-8") as f:
                    try:
                        data = json.load(f)
                    except:
                        continue
                
                # 解析新闻数据
                search_result = data.get("data", {}).get("data", {})
                news_list = search_result.get("newsList", [])
                
                for item in news_list[:5]:  # 每个关键词取前5条
                    news = {
                        "title": item.get("title", ""),
                        "source": item.get("source", "东方财富"),
                        "published_at": item.get("time", datetime.utcnow().isoformat()),
                        "url": item.get("url", ""),
                        "content": item.get("content", "")[:200] + "..." if item.get("content") else "",
                        "keyword": keyword,
                        "sentiment": None,
                    }
                    all_news.append(news)
            
            logger.info(f"{keyword}: 找到 {len(all_news)} 条新闻")
            
        except Exception as e:
            logger.error(f"搜索{keyword}新闻失败: {e}")
    
    # 去重（根据标题）
    seen_titles = set()
    unique_news = []
    for n in all_news:
        if n["title"] not in seen_titles:
            seen_titles.add(n["title"])
            unique_news.append(n)
    
    logger.info(f"mx-search新闻采集完成: {len(unique_news)} 条")
    return unique_news


if __name__ == "__main__":
    # 测试
    print("Testing mx_adapter...")
    
    # 测试个股查询
    print("\n1. 测试个股查询:")
    result = query_cn_stock("宁德时代")
    if result:
        print("   ", result["symbol"], ":", result["close"])
    
    # 测试新闻搜索
    print("\n2. 测试新闻搜索:")
    news = search_mx_news(["宁德时代", "比亚迪"], days=1)
    print("   找到", len(news), "条新闻")
    for n in news[:3]:
        print("   - [", n["source"], "]", n["title"][:40], "...")
