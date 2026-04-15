CREATE TABLE IF NOT EXISTS analytics_load_history (
    batch_path TEXT PRIMARY KEY,
    batch_fingerprint TEXT,
    row_count INTEGER NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ods_news_articles (
    link TEXT PRIMARY KEY,
    batch_path TEXT NOT NULL,
    title TEXT NOT NULL,
    summary TEXT NOT NULL,
    source TEXT NOT NULL,
    published_at TIMESTAMPTZ NOT NULL,
    fetched_at TIMESTAMPTZ NOT NULL,
    ingestion_id TEXT NOT NULL,
    event_date DATE NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ods_news_articles_event_date
    ON ods_news_articles (event_date);

CREATE INDEX IF NOT EXISTS idx_ods_news_articles_source
    ON ods_news_articles (source);

CREATE INDEX IF NOT EXISTS idx_ods_news_articles_batch_path
    ON ods_news_articles (batch_path);

CREATE TABLE IF NOT EXISTS mart_news_daily_source (
    event_date DATE NOT NULL,
    source TEXT NOT NULL,
    article_count INTEGER NOT NULL,
    latest_published_at TIMESTAMPTZ NOT NULL,
    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (event_date, source)
);

CREATE TABLE IF NOT EXISTS mart_article_keywords (
    batch_path TEXT NOT NULL,
    event_date DATE NOT NULL,
    source TEXT NOT NULL,
    link TEXT NOT NULL,
    title TEXT NOT NULL,
    keyword TEXT NOT NULL,
    keyword_normalized TEXT NOT NULL,
    ngram_size INTEGER NOT NULL,
    base_score DOUBLE PRECISION,
    quality_penalty DOUBLE PRECISION,
    article_score DOUBLE PRECISION NOT NULL,
    quality_flags TEXT,
    keyword_score_version TEXT,
    keyword_config_hash TEXT,
    rank_in_article INTEGER NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (batch_path, link, keyword_normalized)
);

CREATE INDEX IF NOT EXISTS idx_mart_article_keywords_event_date
    ON mart_article_keywords (event_date);

CREATE INDEX IF NOT EXISTS idx_mart_article_keywords_source
    ON mart_article_keywords (source);

CREATE INDEX IF NOT EXISTS idx_mart_article_keywords_event_date_source_rank
    ON mart_article_keywords (event_date, source, rank_in_article);

CREATE INDEX IF NOT EXISTS idx_mart_article_keywords_keyword_normalized
    ON mart_article_keywords (keyword_normalized);

CREATE INDEX IF NOT EXISTS idx_mart_article_keywords_link_keyword_loaded_at
    ON mart_article_keywords (link, keyword_normalized, loaded_at DESC);

CREATE TABLE IF NOT EXISTS analytics_keyword_load_history (
    batch_path TEXT PRIMARY KEY,
    keyword_score_version TEXT,
    keyword_config_hash TEXT,
    article_keyword_row_count INTEGER NOT NULL,
    keyword_daily_source_row_count INTEGER NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS mart_keyword_daily_source (
    batch_path TEXT NOT NULL,
    event_date DATE NOT NULL,
    source TEXT NOT NULL,
    keyword TEXT NOT NULL,
    keyword_normalized TEXT NOT NULL,
    ngram_size INTEGER NOT NULL,
    article_count INTEGER NOT NULL,
    base_score DOUBLE PRECISION,
    quality_penalty DOUBLE PRECISION,
    weighted_score DOUBLE PRECISION NOT NULL,
    avg_article_score DOUBLE PRECISION NOT NULL,
    quality_flags TEXT,
    keyword_score_version TEXT,
    keyword_config_hash TEXT,
    source_spread_score DOUBLE PRECISION,
    recency_score DOUBLE PRECISION,
    breakout_score DOUBLE PRECISION,
    final_keyword_score DOUBLE PRECISION,
    rank_in_group INTEGER NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (batch_path, event_date, source, keyword_normalized)
);

CREATE INDEX IF NOT EXISTS idx_mart_keyword_daily_source_event_date
    ON mart_keyword_daily_source (event_date);

CREATE INDEX IF NOT EXISTS idx_mart_keyword_daily_source_source
    ON mart_keyword_daily_source (source);

CREATE INDEX IF NOT EXISTS idx_mart_keyword_daily_source_event_date_source_rank
    ON mart_keyword_daily_source (event_date, source, rank_in_group);

CREATE INDEX IF NOT EXISTS idx_mart_keyword_daily_source_keyword_normalized
    ON mart_keyword_daily_source (keyword_normalized);

CREATE INDEX IF NOT EXISTS idx_mart_keyword_daily_source_lookup
    ON mart_keyword_daily_source (event_date, source, keyword_normalized, loaded_at DESC);

CREATE OR REPLACE VIEW vw_streamlit_article_keywords_latest AS
WITH ranked_keywords AS (
    SELECT
        batch_path,
        event_date,
        source,
        link,
        title,
        keyword,
        keyword_normalized,
        ngram_size,
        base_score,
        quality_penalty,
        article_score,
        quality_flags,
        keyword_score_version,
        keyword_config_hash,
        rank_in_article,
        loaded_at,
        ROW_NUMBER() OVER (
            PARTITION BY link, keyword_normalized
            ORDER BY loaded_at DESC, batch_path DESC, rank_in_article ASC
        ) AS recency_rank
    FROM mart_article_keywords
)
SELECT
    batch_path,
    event_date,
    source,
    link,
    title,
    keyword,
    keyword_normalized,
    ngram_size,
    base_score,
    quality_penalty,
    article_score,
    quality_flags,
    keyword_score_version,
    keyword_config_hash,
    rank_in_article,
    loaded_at
FROM ranked_keywords
WHERE recency_rank = 1;

CREATE OR REPLACE VIEW vw_streamlit_keyword_daily_source_latest AS
WITH ranked_keywords AS (
    SELECT
        batch_path,
        event_date,
        source,
        keyword,
        keyword_normalized,
        ngram_size,
        article_count,
        base_score,
        quality_penalty,
        weighted_score,
        avg_article_score,
        quality_flags,
        keyword_score_version,
        keyword_config_hash,
        source_spread_score,
        recency_score,
        breakout_score,
        final_keyword_score,
        rank_in_group,
        loaded_at,
        ROW_NUMBER() OVER (
            PARTITION BY event_date, source, keyword_normalized
            ORDER BY loaded_at DESC, batch_path DESC, rank_in_group ASC
        ) AS recency_rank
    FROM mart_keyword_daily_source
)
SELECT
    batch_path,
    event_date,
    source,
    keyword,
    keyword_normalized,
    ngram_size,
    article_count,
    base_score,
    quality_penalty,
    weighted_score,
    avg_article_score,
    quality_flags,
    keyword_score_version,
    keyword_config_hash,
    source_spread_score,
    recency_score,
    breakout_score,
    final_keyword_score,
    rank_in_group,
    loaded_at
FROM ranked_keywords
WHERE recency_rank = 1;

CREATE OR REPLACE VIEW vw_streamlit_keyword_daily_overall_latest AS
WITH latest_source_keywords AS (
    SELECT
        event_date,
        source,
        keyword,
        keyword_normalized,
        ngram_size,
        article_count,
        weighted_score,
        avg_article_score,
        final_keyword_score,
        keyword_score_version,
        keyword_config_hash
    FROM vw_streamlit_keyword_daily_source_latest
)
SELECT
    event_date,
    keyword,
    keyword_normalized,
    MAX(ngram_size) AS ngram_size,
    COUNT(DISTINCT source) AS source_count,
    SUM(article_count) AS article_count,
    SUM(weighted_score) AS weighted_score,
    AVG(avg_article_score) AS avg_article_score,
    SUM(final_keyword_score) AS final_keyword_score,
    MAX(keyword_score_version) AS keyword_score_version,
    MAX(keyword_config_hash) AS keyword_config_hash,
    ROW_NUMBER() OVER (
        PARTITION BY event_date
        ORDER BY
            SUM(final_keyword_score) DESC,
            SUM(article_count) DESC,
            MAX(ngram_size) DESC,
            keyword_normalized ASC
    ) AS rank_in_day
FROM latest_source_keywords
GROUP BY event_date, keyword, keyword_normalized;
