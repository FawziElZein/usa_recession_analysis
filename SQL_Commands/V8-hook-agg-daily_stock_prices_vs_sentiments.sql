CREATE TABLE IF NOT EXISTS target_schema.agg_daily_stock_prices_vs_sentiments(
    date_ticker VARCHAR(100) PRIMARY KEY NOT NULL,
    date DATE,
    ticker VARCHAR(100),
    stock_price DOUBLE PRECISION,
    neg FLOAT,
    neu FLOAT,
    pos FLOAT,
    compound FLOAT
);

CREATE INDEX IF NOT EXISTS idx_date_ticker ON target_schema.agg_daily_stock_prices_vs_sentiments(date_ticker);

TRUNCATE TABLE target_schema.agg_daily_stock_prices_vs_sentiments;

INSERT INTO target_schema.agg_daily_stock_prices_vs_sentiments
(
    SELECT
        CONCAT(date,'-',ticker) AS date_ticker,
        date,
        ticker,
        stock_price,
        ROUND(CAST(AVG(neg) AS DECIMAL),2) AS avg_negative_sentiment,
        ROUND(CAST(AVG(neu) AS DECIMAL),2) AS avg_neutral_sentiment,
        ROUND(CAST(AVG(pos) AS DECIMAL),2) AS avg_positive_sentiment,
        ROUND(CAST(AVG(compound) AS DECIMAL),2) avg_compound_result
    FROM target_schema.fact_stock_prices_vs_sentiments
    GROUP BY
        date, ticker,stock_price
    ORDER BY
        date DESC,ticker
);
