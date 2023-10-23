CREATE TABLE IF NOT EXISTS target_schema.agg_daily_stock_prices_vs_sentiments(
    date_ticker VARCHAR(100) PRIMARY KEY NOT NULL,
    date DATE,
    ticker VARCHAR(6),
    stock_price DECIMAL,
    price_trend_value DECIMAL,
    price_trend_percentage DECIMAL,
    neg FLOAT,
    neu FLOAT,
    pos FLOAT,
    compound FLOAT
);

CREATE INDEX IF NOT EXISTS idx_date_ticker ON target_schema.agg_daily_stock_prices_vs_sentiments(date_ticker);

TRUNCATE TABLE target_schema.agg_daily_stock_prices_vs_sentiments;

WITH CTE_AGG_DAILY_STOCK_PRICES_VS_SENTIMENTS AS (
    SELECT
        CONCAT(date,'-',ticker) AS date_ticker,
        date,
        ticker,
        CAST(stock_price AS DECIMAL) AS stock_price,
        ROUND(CAST(AVG(neg) AS DECIMAL),2) AS avg_negative_sentiment,
        ROUND(CAST(AVG(neu) AS DECIMAL),2) AS avg_neutral_sentiment,
        ROUND(CAST(AVG(pos) AS DECIMAL),2) AS avg_positive_sentiment,
        ROUND(CAST(AVG(compound) AS DECIMAL),2) avg_compound_result
    FROM target_schema.fact_stock_prices_vs_sentiments
    GROUP BY
        date, ticker,stock_price
)

INSERT INTO target_schema.agg_daily_stock_prices_vs_sentiments
(
SELECT
	date_ticker,
	date,
	ticker,
	stock_price,
	ROUND(stock_price - LAG(stock_price) OVER (PARTITION BY ticker ORDER BY date),2) AS price_trend_value,
	ROUND((stock_price - LAG(stock_price) OVER (PARTITION BY ticker ORDER BY date))/ABS(LAG(stock_price) OVER (PARTITION BY ticker ORDER BY date)),3)*100 AS price_trend_percentage,
	avg_negative_sentiment,
	avg_neutral_sentiment,
	avg_positive_sentiment,
	avg_compound_result
FROM CTE_AGG_DAILY_STOCK_PRICES_VS_SENTIMENTS
ORDER BY
	date_ticker
);
