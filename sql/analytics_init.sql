CREATE TABLE IF NOT EXISTS analytics_load_history (
    batch_path TEXT PRIMARY KEY,
    row_count INTEGER NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ods_news_articles (
    link TEXT PRIMARY KEY,
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

CREATE TABLE IF NOT EXISTS mart_news_daily_source (
    event_date DATE NOT NULL,
    source TEXT NOT NULL,
    article_count INTEGER NOT NULL,
    latest_published_at TIMESTAMPTZ NOT NULL,
    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (event_date, source)
);
