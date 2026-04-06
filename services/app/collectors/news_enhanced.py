"""
增强版新闻采集器 - 多维度精准情绪分析
覆盖：地缘政治、能源政策、市场动态、技术突破
"""

import logging
import requests
from datetime import date, datetime, timedelta
from database import get_db

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("energypulse.news_enhanced")

# Tavily API Key (使用现有的)
TAVILY_API_KEY = "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"

# 新闻分类与关键词配置
NEWS_CATEGORIES = {
    "geopolitics": {
        "name": "地缘政治",
        "keywords": [
            "war", "conflict", "sanctions", "Middle East", "Russia Ukraine",
            "Iran", "Saudi Arabia", "OPEC", "supply disruption", "embargo",
            "军事冲突", "制裁", "中东", "俄乌", "伊朗", "石油禁运"
        ],
        "weight": 1.5,  # 地缘新闻权重更高
        "impact_map": {
            "war": {"energy": "bullish", "sentiment": -0.8},
            "sanctions": {"energy": "bullish", "sentiment": -0.5},
            "supply disruption": {"energy": "bullish", "sentiment": -0.7},
            "peace talk": {"energy": "bearish", "sentiment": 0.3},
        }
    },
    "policy": {
        "name": "能源政策",
        "keywords": [
            "carbon neutral", "energy transition", "climate policy", "ESG",
            "coal phase out", "renewable energy", "green deal", "COP",
            "碳中和", "能源转型", "双碳", "煤电", "可再生能源", "环保政策"
        ],
        "weight": 1.2,
        "impact_map": {
            "coal phase out": {"coal": "bearish", "renewable": "bullish", "sentiment": -0.6},
            "carbon neutral": {"coal": "bearish", "sentiment": -0.4},
            "energy transition": {"traditional": "bearish", "new": "bullish"},
        }
    },
    "market": {
        "name": "市场动态",
        "keywords": [
            "oil price", "natural gas", "crude oil", "OPEC production",
            "inventory", "demand forecast", "supply chain",
            "油价", "天然气", "原油库存", "产量", "需求预测"
        ],
        "weight": 1.0,
    },
    "finance": {
        "name": "金融投资",
        "keywords": [
            "energy ETF", "dividend", "stock performance", "earnings",
            "investment", "fund flow", "hedge fund",
            "股息", "财报", "资金流向", "机构持仓"
        ],
        "weight": 0.8,
    }
}


def search_news_by_category(category, days=1):
    """按分类搜索新闻"""
    config = NEWS_CATEGORIES[category]
    keywords = " OR ".join(config["keywords"][:5])  # 前5个关键词
    
    url = "https://api.tavily.com/search"
    headers = {"Content-Type": "application/json"}
    
    query = f"({keywords}) energy last {days} days"
    
    payload = {
        "api_key": TAVILY_API_KEY,
        "query": query,
        "search_depth": "advanced",
        "include_domains": [
            "reuters.com", "bloomberg.com", "cnbc.com", "ft.com",
            "wsj.com", "energyintel.com", "worldoil.com", "oilprice.com",
            "xinhuanet.com", "sina.com.cn", "eastmoney.com"
        ],
        "max_results": 10,
    }
    
    try:
        resp = requests.post(url, json=payload, headers=headers, timeout=30)
        data = resp.json()
        return data.get("results", [])
    except Exception as e:
        logger.error(f"搜索{category}新闻失败: {e}")
        return []


def analyze_sentiment_enhanced(title, content, category):
    """增强版情绪分析"""
    config = NEWS_CATEGORIES.get(category, {})
    impact_map = config.get("impact_map", {})
    
    # 基础情绪词典
    positive_words = ["增长", "上涨", "突破", "利好", "强劲", "复苏", "optimistic", "surge", "rally", "boost"]
    negative_words = ["下跌", "暴跌", "衰退", "危机", "制裁", "冲突", "战争", "crash", "plunge", "crisis", "sanctions"]
    
    # 计算基础情绪分
    text = (title + " " + content).lower()
    pos_count = sum(1 for w in positive_words if w in text)
    neg_count = sum(1 for w in negative_words if w in text)
    
    base_score = (pos_count - neg_count) / max(pos_count + neg_count, 1)
    
    # 检查特定事件影响
    event_impact = None
    for event, impact in impact_map.items():
        if event.lower() in text:
            event_impact = impact
            break
    
    # 综合评分
    weight = config.get("weight", 1.0)
    final_score = base_score * weight
    
    # 情绪标签
    if final_score > 0.3:
        sentiment = "positive"
    elif final_score < -0.3:
        sentiment = "negative"
    else:
        sentiment = "neutral"
    
    return {
        "score": round(final_score, 2),
        "label": sentiment,
        "event_impact": event_impact,
        "category": category,
    }


def collect_enhanced_news():
    """采集增强版新闻"""
    logger.info("开始采集增强版新闻...")
    db = get_db()
    today = date.today().isoformat()
    all_news = []
    
    for category in NEWS_CATEGORIES.keys():
        try:
            results = search_news_by_category(category, days=1)
            
            for item in results:
                title = item.get("title", "")
                content = item.get("content", "")[:500]
                
                # 情绪分析
                sentiment = analyze_sentiment_enhanced(title, content, category)
                
                news = {
                    "title": title,
                    "source": item.get("source", "Unknown"),
                    "url": item.get("url", ""),
                    "published_at": item.get("published_date", datetime.utcnow().isoformat()),
                    "category": category,
                    "sentiment_score": sentiment["score"],
                    "sentiment_label": sentiment["label"],
                    "event_impact": json.dumps(sentiment["event_impact"]) if sentiment["event_impact"] else None,
                    "summary": content[:200],
                    "collected_at": datetime.utcnow().isoformat(),
                }
                all_news.append(news)
            
            logger.info(f"{category}: 采集 {len(results)} 条")
            
        except Exception as e:
            logger.error(f"采集{category}失败: {e}")
    
    # 去重并存储
    seen = set()
    unique_news = []
    for n in all_news:
        if n["title"] not in seen:
            seen.add(n["title"])
            unique_news.append(n)
    
    # 保存到数据库
    for n in unique_news:
        sql = """
            INSERT INTO news_sentiment (title, source, url, published_at, category, 
                                       sentiment_score, sentiment_label, event_impact, summary, collected_at)
            VALUES (%(title)s, %(source)s, %(url)s, %(published_at)s, %(category)s,
                    %(sentiment_score)s, %(sentiment_label)s, %(event_impact)s, %(summary)s, %(collected_at)s)
            ON CONFLICT (url) DO UPDATE SET
                sentiment_score = EXCLUDED.sentiment_score,
                sentiment_label = EXCLUDED.sentiment_label
        """
        db.execute(sql, n)
    
    logger.info(f"新闻采集完成: {len(unique_news)} 条")
    return unique_news


if __name__ == "__main__":
    import json
    news = collect_enhanced_news()
    print(f"\n采集完成: {len(news)} 条")
    
    # 按分类统计
    by_category = {}
    for n in news:
        cat = n["category"]
        by_category[cat] = by_category.get(cat, 0) + 1
    
    print("\n分类统计:")
    for cat, count in by_category.items():
        print(f"  {cat}: {count}条")
