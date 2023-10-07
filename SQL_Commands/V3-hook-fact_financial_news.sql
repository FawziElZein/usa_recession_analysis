CREATE TABLE IF NOT EXISTS target_schema.fact_financial_news(
    news_title TEXT PRIMARY KEY NOT NULL,
    ticker VARCHAR(100),
    date DATE,
    time TIME,
    text TEXT
);

CREATE INDEX IF NOT EXISTS idx_news_title  ON target_schema.fact_financial_news(news_title);

INSERT INTO target_schema.fact_financial_news
(
    SELECT
        title,
        ticker,
        date,
        time,
        text
    FROM target_schema.stg_finviz_financial_news
)
ON CONFLICT (news_title)
DO UPDATE SET
    ticker = EXCLUDED.ticker,
    date = EXCLUDED.date,
    time = EXCLUDED.time,
    text = EXCLUDED.text;