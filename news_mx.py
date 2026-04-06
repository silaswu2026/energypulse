"""
增强版新闻采集器 - 基于妙想 mx-search
覆盖：地缘政治、能源政策、市场动态
"""
import os
import sys
sys.path.insert(0, "/app")
sys.path.insert(0, "/app/skills/mx-search")

import json
import logging
from datetime import date, datetime
from database import get_db

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("energypulse.news_mx")

from mx_search import MXSearch

# 新闻分类与搜索查询配置
NEWS_CATEGORIES = {
    "geopolitics": {
        "name": "地缘政治",
        "queries": [
            "中东冲突 石油供应 2025",
            "俄乌战争 能源制裁 影响",
            "伊朗以色列 地缘风险 原油",
            "OPEC减产 地缘政治因素"
        ],
        "weight": 1.5,
        "keywords": ["战争", "冲突", "制裁", "袭击", "导弹", "报复", "地缘", "紧张"]
    },
    "policy": {
        "name": "能源政策",
        "queries": [
            "碳中和 能源转型 政策 中国",
            "煤电政策 产能淘汰 2025",
            "新能源政策 风光电 投资"
        ],
        "weight": 1.2,
        "keywords": ["政策", "碳中和", "转型", "淘汰", "产能", "双碳", "绿色"]
    },
    "market": {
        "name": "市场动态",
        "queries": [
            "原油价格 最新行情 分析",
            "煤炭价格 动力煤 走势",
            "天然气价格 供需 预测"
        ],
        "weight": 1.0,
        "keywords": ["价格", "上涨", "下跌", "供需", "库存", "产量"]
    }
}


def analyze_sentiment_enhanced(title, content, category):
    """增强版情绪分析"""
    config = NEWS_CATEGORIES.get(category, {})
    keywords = config.get("keywords", [])
    weight = config.get("weight", 1.0)
    
    # 合并文本
    text = (title + " " + content).lower()
    
    # 积极/消极词典
    positive = ["上涨", "反弹", "突破", "利好", "强劲", "复苏", "创新高", "增产", "需求增长", 
                "rally", "surge", "rise", "gain", "boost", "strong"]
    negative = ["下跌", "暴跌", "制裁", "冲突", "战争", "危机", "供应中断", "减产", "需求疲软",
                "plunge", "crash", "fall", "drop", "sanctions", "conflict", "disruption"]
    
    pos_count = sum(1 for w in positive if w in text)
    neg_count = sum(1 for w in negative if w in text)
    
    # 地缘关键词检测
    geo_impact = 0
    for kw in keywords:
        if kw in text:
            geo_impact += 1
    
    # 计算基础情绪分 (-1 到 1)
    total_signals = pos_count + neg_count + geo_impact * 0.5
    if total_signals == 0:
        base_score = 0
    else:
        base_score = (pos_count - neg_count) / total_signals
    
    # 应用分类权重
    final_score = base_score * weight
    
    # 限制范围
    final_score = max(-1.0, min(1.0, final_score))
    
    # 情绪标签
    if final_score > 0.3:
        label = "positive"
    elif final_score < -0.3:
        label = "negative"
    else:
        label = "neutral"
    
    # 检测特定事件类型
    event_type = None
    if any(w in text for w in ["战争", "冲突", "war", "conflict"]):
        event_type = "war_conflict"
    elif any(w in text for w in ["制裁", "sanctions", "embargo"]):
        event_type = "sanctions"
    elif any(w in text for w in ["OPEC", "减产", "production cut"]):
        event_type = "supply_adjustment"
    
    return {
        "score": round(final_score, 2),
        "label": label,
        "event_type": event_type,
        "category": category
    }


def collect_mx_news():
    """使用 mx-search 采集新闻"""
    logger.info("开始采集妙想新闻...")
    
    try:
        client = MXSearch()
    except Exception as e:
        logger.error(f"初始化MXSearch失败: {e}")
        return []
    
    db = get_db()
    all_news = []
    
    for cat_key, config in NEWS_CATEGORIES.items():
        logger.info(f"采集分类: {config[name]}")
        
        for query in config["queries"]:
            try:
                result = client.search(query)
                content = MXSearch.extract_content(result)
                
                # 解析内容作为新闻列表
                try:
                    news_list = json.loads(content) if content.startswith("[") else []
                except:
                    # 如果不是JSON数组，提取标题
                    news_list = [{"title": content[:100], "content": content}]
                
                if not isinstance(news_list, list):
                    news_list = [{"title": str(content)[:100], "content": str(content)}]
                
                for news in news_list[:5]:  # 每查询最多5条
                    if isinstance(news, dict):
                        title = news.get("title", "") or str(news)[:100]
                        content = news.get("content", "") or news.get("summary", "")
                    else:
                        title = str(news)[:100]
                        content = ""
                    
                    # 跳过太短的
                    if len(title) < 10:
                        continue
                    
                    # 情绪分析
                    sentiment = analyze_sentiment_enhanced(title, content, cat_key)
                    
                    news_item = {
                        "title": title[:200],
                        "source": "mx-search",
                        "url": news.get("url", "") if isinstance(news, dict) else "",
                        "published_at": datetime.utcnow().isoformat(),
                        "category": cat_key,
                        "sentiment_score": sentiment["score"],
                        "sentiment_label": sentiment["label"],
                        "event_impact": sentiment["event_type"],
                        "summary": content[:300] if content else title[:100],
                        "collected_at": datetime.utcnow().isoformat()
                    }
                    all_news.append(news_item)
                
                logger.info(f"  {query[:30]}... -> {len(news_list)} 条")
                
            except Exception as e:
                logger.error(f"查询失败 {query}: {e}")
    
    # 去重
    seen = set()
    unique = []
    for n in all_news:
        if n["title"] not in seen:
            seen.add(n["title"])
            unique.append(n)
    
    # 保存到数据库
    for n in unique:
        sql = """
            INSERT INTO news_sentiment (title, source, url, published_at, category,
                                       sentiment_score, sentiment_label, event_impact, summary, collected_at)
            VALUES (%(title)s, %(source)s, %(url)s, %(published_at)s, %(category)s,
                    %(sentiment_score)s, %(sentiment_label)s, %(event_impact)s, %(summary)s, %(collected_at)s)
            ON CONFLICT (url) DO UPDATE SET
                sentiment_score = EXCLUDED.sentiment_score,
                sentiment_label = EXCLUDED.sentiment_label,
                category = EXCLUDED.category
        """
        try:
            db.execute(sql, n)
        except Exception as e:
            # URL可能为空或重复，尝试更新
            pass
    
    logger.info(f"新闻采集完成: {len(unique)} 条")
    return unique


if __name__ == "__main__":
    news = collect_mx_news()
    print(f"\\n采集完成: {len(news)} 条")
    
    # 按分类统计
    stats = {}
    for n in news:
        cat = n["category"]
        stats[cat] = stats.get(cat, 0) + 1
    
    print("\\n分类统计:")
    for cat, count in stats.items():
        print(f"  {cat}: {count}条")
