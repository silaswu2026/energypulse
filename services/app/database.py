"""
EnergyPulse 数据库连接模块
基于 psycopg2,连接池管理,统一增删改查。
"""

import os
import json
import logging
import psycopg2
import psycopg2.extras
from contextlib import contextmanager

logger = logging.getLogger("energypulse.db")

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://epuser:changeme@localhost:5432/energypulse"
)


class Database:
    """简单数据库封装,单连接复用(2C4G服务器不需要连接池)"""

    def __init__(self, dsn: str = None):
        self.dsn = dsn or DATABASE_URL
        self._conn = None

    def _get_conn(self):
        if self._conn is None or self._conn.closed:
            self._conn = psycopg2.connect(self.dsn)
            self._conn.autocommit = False
            logger.info("数据库连接已建立")
        return self._conn

    @contextmanager
    def cursor(self):
        conn = self._get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            yield cur
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()

    def execute(self, sql: str, params=None):
        with self.cursor() as cur:
            cur.execute(sql, params)
            return cur.rowcount

    def query(self, sql: str, params=None) -> list[dict]:
        with self.cursor() as cur:
            cur.execute(sql, params)
            return [dict(row) for row in cur.fetchall()]

    def query_one(self, sql: str, params=None) -> dict | None:
        rows = self.query(sql, params)
        return rows[0] if rows else None

    def upsert_macro(self, records: list[dict]):
        """批量upsert宏观指标(幂等)"""
        if not records:
            return 0
        sql = """
            INSERT INTO macro_indicators
                (time, source_id, series_id, series_name, value, unit, period_type,
                 raw_url, raw_hash, collected_at, verified, degraded)
            VALUES
                (%(time)s, %(source_id)s, %(series_id)s, %(series_name)s,
                 %(value)s, %(unit)s, %(period_type)s,
                 %(raw_url)s, %(raw_hash)s, NOW(), %(verified)s, %(degraded)s)
            ON CONFLICT (time, source_id, series_id)
            DO UPDATE SET value = EXCLUDED.value,
                          collected_at = NOW(),
                          verified = EXCLUDED.verified,
                          degraded = EXCLUDED.degraded
            WHERE macro_indicators.value != EXCLUDED.value
        """
        with self.cursor() as cur:
            psycopg2.extras.execute_batch(cur, sql, records)
            return len(records)

    def upsert_stock(self, records: list[dict]):
        """批量upsert股票行情(幂等)"""
        if not records:
            return 0
        sql = """
            INSERT INTO stock_daily
                (trade_date, market, symbol, name, open, high, low, close,
                 volume, turnover, change_pct, collected_at, degraded)
            VALUES
                (%(trade_date)s, %(market)s, %(symbol)s, %(name)s,
                 %(open)s, %(high)s, %(low)s, %(close)s,
                 %(volume)s, %(turnover)s, %(change_pct)s, NOW(), %(degraded)s)
            ON CONFLICT (trade_date, market, symbol)
            DO UPDATE SET close = EXCLUDED.close,
                          change_pct = EXCLUDED.change_pct,
                          collected_at = NOW()
        """
        with self.cursor() as cur:
            psycopg2.extras.execute_batch(cur, sql, records)
            return len(records)

    def upsert_commodity(self, records: list[dict]):
        """批量upsert大宗商品(幂等)"""
        if not records:
            return 0
        sql = """
            INSERT INTO commodity_daily
                (trade_date, commodity_id, value, unit, change_pct,
                 is_satellite, source, collected_at, degraded)
            VALUES
                (%(trade_date)s, %(commodity_id)s, %(value)s, %(unit)s,
                 %(change_pct)s, %(is_satellite)s, %(source)s, NOW(), %(degraded)s)
            ON CONFLICT (trade_date, commodity_id)
            DO UPDATE SET value = EXCLUDED.value,
                          change_pct = EXCLUDED.change_pct,
                          collected_at = NOW()
        """
        with self.cursor() as cur:
            psycopg2.extras.execute_batch(cur, sql, records)
            return len(records)

    def upsert_news(self, records: list[dict]):
        """批量插入新闻(去重by raw_hash)"""
        if not records:
            return 0
        sql = """
            INSERT INTO news_sentiment
                (source, title, summary, url, published_at, language,
                 category, relevance, sentiment_score, sentiment_label, raw_hash)
            VALUES
                (%(source)s, %(title)s, %(summary)s, %(url)s, %(published_at)s,
                 %(language)s, %(category)s, %(relevance)s, %(sentiment_score)s,
                 %(sentiment_label)s, %(raw_hash)s)
            ON CONFLICT (raw_hash) DO NOTHING
        """
        with self.cursor() as cur:
            psycopg2.extras.execute_batch(cur, sql, records)
            return len(records)

    def upsert_weather(self, records: list[dict]):
        """批量插入天气HDD/CDD数据"""
        if not records:
            return 0
        sql = """
            INSERT INTO weather_demand
                (date, region, hdd, cdd, temp_avg_f, source)
            VALUES
                (%(date)s, %(region)s, %(hdd)s, %(cdd)s, %(temp_avg_f)s, %(source)s)
            ON CONFLICT (date, region)
            DO UPDATE SET hdd = EXCLUDED.hdd,
                          cdd = EXCLUDED.cdd,
                          temp_avg_f = EXCLUDED.temp_avg_f,
                          source = EXCLUDED.source
        """
        with self.cursor() as cur:
            psycopg2.extras.execute_batch(cur, sql, records)
            return len(records)

    def insert_log(self, collector_id: str, channel: str, status: str,
                   records_count: int = 0, error_message: str = None,
                   duration_ms: int = 0):
        """记录采集日志"""
        self.execute(
            """INSERT INTO collection_log
               (collector_id, channel, status, records_count, error_message, duration_ms)
               VALUES (%s, %s, %s, %s, %s, %s)""",
            [collector_id, channel, status, records_count, error_message, duration_ms]
        )

    def close(self):
        if self._conn and not self._conn.closed:
            self._conn.close()
            logger.info("数据库连接已关闭")


# 全局单例
_db_instance = None

def get_db() -> Database:
    global _db_instance
    if _db_instance is None:
        _db_instance = Database()
    return _db_instance
