-- =============================================
-- EnergyPulse Database Initialization
-- PostgreSQL 16 + TimescaleDB
-- =============================================

-- Enable TimescaleDB
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- =============================================
-- 1. 宏观指标时序表
-- =============================================
CREATE TABLE IF NOT EXISTS macro_indicators (
    time         TIMESTAMPTZ NOT NULL,
    source_id    VARCHAR(32) NOT NULL,
    series_id    VARCHAR(64) NOT NULL,
    series_name  VARCHAR(256),
    value        DOUBLE PRECISION NOT NULL,
    unit         VARCHAR(32),
    period_type  VARCHAR(16),
    raw_url      TEXT,
    raw_hash     VARCHAR(32),
    collected_at TIMESTAMPTZ DEFAULT NOW(),
    verified     BOOLEAN DEFAULT TRUE,
    degraded     BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (time, source_id, series_id)
);
SELECT create_hypertable('macro_indicators', 'time', if_not_exists => TRUE);

-- =============================================
-- 2. 股票行情日度表
-- =============================================
CREATE TABLE IF NOT EXISTS stock_daily (
    trade_date   DATE NOT NULL,
    market       VARCHAR(8) NOT NULL,
    symbol       VARCHAR(16) NOT NULL,
    name         VARCHAR(64),
    open         DOUBLE PRECISION,
    high         DOUBLE PRECISION,
    low          DOUBLE PRECISION,
    close        DOUBLE PRECISION NOT NULL,
    volume       BIGINT,
    turnover     DOUBLE PRECISION,
    change_pct   DOUBLE PRECISION,
    ma5          DOUBLE PRECISION,
    ma20         DOUBLE PRECISION,
    ma60         DOUBLE PRECISION,
    rsi14        DOUBLE PRECISION,
    macd         DOUBLE PRECISION,
    macd_signal  DOUBLE PRECISION,
    macd_hist    DOUBLE PRECISION,
    collected_at TIMESTAMPTZ DEFAULT NOW(),
    degraded     BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (trade_date, market, symbol)
);

-- =============================================
-- 3. 大宗商品与资本指标日度表
-- =============================================
CREATE TABLE IF NOT EXISTS commodity_daily (
    trade_date   DATE NOT NULL,
    commodity_id VARCHAR(32) NOT NULL,
    value        DOUBLE PRECISION NOT NULL,
    unit         VARCHAR(32),
    change_pct   DOUBLE PRECISION,
    is_satellite BOOLEAN DEFAULT FALSE,
    anomaly_flag BOOLEAN DEFAULT FALSE,
    source       VARCHAR(32),
    collected_at TIMESTAMPTZ DEFAULT NOW(),
    degraded     BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (trade_date, commodity_id)
);

-- =============================================
-- 4. 天气/电力需求表
-- =============================================
CREATE TABLE IF NOT EXISTS weather_demand (
    date         DATE NOT NULL,
    region       VARCHAR(32) NOT NULL,
    hdd          DOUBLE PRECISION,
    cdd          DOUBLE PRECISION,
    temp_avg_f   DOUBLE PRECISION,
    source       VARCHAR(32) DEFAULT 'VisualCrossing',
    collected_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (date, region)
);

-- =============================================
-- 5. 新闻舆情表
-- =============================================
CREATE TABLE IF NOT EXISTS news_sentiment (
    id           BIGSERIAL PRIMARY KEY,
    published_at TIMESTAMPTZ,
    source       VARCHAR(64) NOT NULL,
    title        TEXT NOT NULL,
    summary      TEXT,
    url          TEXT,
    language     VARCHAR(8),
    category     VARCHAR(32),
    relevance    VARCHAR(16),
    sentiment_score  DOUBLE PRECISION,
    sentiment_label  VARCHAR(16),
    sentiment_model  VARCHAR(32),
    raw_hash     VARCHAR(64) UNIQUE,
    collected_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_news_published ON news_sentiment (published_at DESC);
CREATE INDEX IF NOT EXISTS idx_news_category ON news_sentiment (category, relevance);

-- =============================================
-- 6. 资金流向表
-- =============================================
CREATE TABLE IF NOT EXISTS capital_flow (
    trade_date       DATE NOT NULL,
    symbol           VARCHAR(16) NOT NULL,
    northbound_net   DOUBLE PRECISION,
    margin_balance   DOUBLE PRECISION,
    short_balance    DOUBLE PRECISION,
    is_dragon_tiger  BOOLEAN DEFAULT FALSE,
    dragon_detail    JSONB,
    collected_at     TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (trade_date, symbol)
);

-- =============================================
-- 7. 股息与基本面快照表
-- =============================================
CREATE TABLE IF NOT EXISTS fundamental_snapshot (
    snapshot_date  DATE NOT NULL,
    symbol         VARCHAR(16) NOT NULL,
    pe_ttm         DOUBLE PRECISION,
    pb             DOUBLE PRECISION,
    dividend_yield DOUBLE PRECISION,
    payout_ratio   DOUBLE PRECISION,
    roe            DOUBLE PRECISION,
    debt_ratio     DOUBLE PRECISION,
    market_cap     DOUBLE PRECISION,
    source         VARCHAR(32),
    PRIMARY KEY (snapshot_date, symbol)
);

-- =============================================
-- 8. 报告存档表
-- =============================================
CREATE TABLE IF NOT EXISTS reports (
    id           SERIAL PRIMARY KEY,
    report_type  VARCHAR(16) NOT NULL,
    report_date  DATE NOT NULL,
    title        VARCHAR(256) NOT NULL,
    content_md   TEXT NOT NULL,
    content_html TEXT,
    ai_model     VARCHAR(32),
    ai_prompt_hash VARCHAR(32),
    data_snapshot JSONB,
    direction    VARCHAR(16),
    score        INTEGER,
    published    BOOLEAN DEFAULT FALSE,
    published_at TIMESTAMPTZ,
    wechat_sent  BOOLEAN DEFAULT FALSE,
    created_at   TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(report_type, report_date)
);
CREATE INDEX IF NOT EXISTS idx_reports_type_date ON reports (report_type, report_date DESC);

-- =============================================
-- 9. 数据采集模板表
-- =============================================
CREATE TABLE IF NOT EXISTS collection_templates (
    id                   SERIAL PRIMARY KEY,
    template_name        VARCHAR(64) NOT NULL,
    is_active            BOOLEAN DEFAULT FALSE,
    weight_macro         DOUBLE PRECISION DEFAULT 0.20,
    weight_sentiment     DOUBLE PRECISION DEFAULT 0.10,
    weight_us_stock      DOUBLE PRECISION DEFAULT 0.20,
    weight_commodity     DOUBLE PRECISION DEFAULT 0.20,
    weight_cn_stock      DOUBLE PRECISION DEFAULT 0.20,
    weight_carbon_esg    DOUBLE PRECISION DEFAULT 0.10,
    focus_keywords       JSONB DEFAULT '[]'::jsonb,
    satellite_thresholds JSONB DEFAULT '{
        "HSI": 2.0, "NIKKEI225": 2.0,
        "COMEX_GOLD": 2.0, "LME_COPPER": 3.0,
        "CN_CEA_CARBON": 5.0
    }'::jsonb,
    notes                TEXT,
    updated_by           VARCHAR(64),
    updated_at           TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO collection_templates (template_name, is_active, notes)
VALUES ('default', TRUE, '标准六维度均衡采集')
ON CONFLICT DO NOTHING;

-- =============================================
-- 10. 系统运行日志表
-- =============================================
CREATE TABLE IF NOT EXISTS collection_log (
    id            BIGSERIAL PRIMARY KEY,
    collector_id  VARCHAR(64) NOT NULL,
    channel       VARCHAR(16) DEFAULT 'primary',
    run_at        TIMESTAMPTZ DEFAULT NOW(),
    status        VARCHAR(16) NOT NULL,
    records_count INTEGER DEFAULT 0,
    error_message TEXT,
    duration_ms   INTEGER
);
CREATE INDEX IF NOT EXISTS idx_log_collector ON collection_log (collector_id, run_at DESC);
