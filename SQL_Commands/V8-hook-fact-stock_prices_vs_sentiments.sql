
CREATE TABLE IF NOT EXISTS target_schema.fact_stock_prices_vs_sentiments(

    ticker_title TEXT PRIMARY KEY NOT NULL,
    ticker VARCHAR(100),
    title TEXT,
    date DATE,
    stock_price DOUBLE PRECISION,
    neg FLOAT,
    neu FLOAT,
    pos FLOAT,
    compound FLOAT,
    url TEXT,
    text TEXT
);

CREATE INDEX IF NOT EXISTS idx_ticker_title ON target_schema.fact_stock_prices_vs_sentiments(ticker_title);

TRUNCATE TABLE target_schema.fact_stock_prices_vs_sentiments;

INSERT INTO target_schema.fact_stock_prices_vs_sentiments
(
    SELECT
        news_sentiments.ticker_title,
        news_sentiments.ticker,
        news_sentiments.title,
        news_sentiments.date,
        ROUND(CAST ((faang_stock_price.high + faang_stock_price.low)/2 AS DECIMAL),2) AS stock_price,
        news_sentiments.neg,
        news_sentiments.neu,
        news_sentiments.pos,
        news_sentiments.compound,
        news_sentiments.url,
        news_sentiments.text
    FROM target_schema.fact_financial_news AS news_sentiments
	INNER JOIN target_schema.dim_faang_stock_price AS faang_stock_price
	ON news_sentiments.ticker = faang_stock_price.ticker
	AND news_sentiments.date = faang_stock_price.formatted_date
    ORDER BY 
        news_sentiments.ticker,news_sentiments.date,news_sentiments.time
);
