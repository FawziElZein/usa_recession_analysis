CREATE TABLE IF NOT EXISTS target_schema.stg_finviz_financial_news(

    news_id SERIAL PRIMARY KEY NOT NULL,
    ticker VARCHAR(100),
    date DATE,
    time TIME,
    title TEXT,
    text TEXT
);