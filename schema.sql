CREATE TABLE IF NOT EXISTS news_events_clean (
    event_id TEXT,
    source TEXT,
    title TEXT,
    summary TEXT,
    url TEXT,
    published_at TIMESTAMP,
    ingested_at TIMESTAMP,
    category TEXT,
    language TEXT,
    location TEXT,
    author TEXT,
    entities TEXT,
    sentiment REAL,
    relevance_score REAL
);
CREATE TABLE IF NOT EXISTS dq_metrics (
    metric TEXT,
    value REAL,
    computed_at TEXT,
    run_name TEXT
);
CREATE INDEX IF NOT EXISTS idx_news_pub ON news_events_clean(published_at);
CREATE INDEX IF NOT EXISTS idx_news_ing ON news_events_clean(ingested_at);
CREATE INDEX IF NOT EXISTS idx_news_title_url ON news_events_clean(title, url);
CREATE INDEX IF NOT EXISTS idx_news_event_id ON news_events_clean(event_id);
