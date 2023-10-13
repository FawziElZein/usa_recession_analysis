CREATE TABLE IF NOT EXISTS target_schema.fact_financial_news(
    ticker_title TEXT PRIMARY KEY NOT NULL,
    ticker VARCHAR(100),
    title TEXT,
    date DATE,
    time TIME,
    neg FLOAT,
    neu FLOAT,
    pos FLOAT,
    compound FLOAT,
    url TEXT,
    text TEXT

);

CREATE INDEX IF NOT EXISTS idx_ticker_title ON target_schema.fact_financial_news(ticker_title);

