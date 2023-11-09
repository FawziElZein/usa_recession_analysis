CREATE TABLE IF NOT EXISTS target_schema.agg_monthly_stock_prices_vs_sentiments
(
    date_ticker TEXT PRIMARY KEY NOT NULL,
    date DATE,
    ticker VARCHAR(10),
    stock_price DOUBLE PRECISION,
    neg FLOAT,
    neu FLOAT,
    pos FLOAT,
    compound FLOAT
);

CREATE INDEX IF NOT EXISTS idx_agg_monthly_stock_prices_vs_sentiments_date_ticker ON target_schema.agg_monthly_stock_prices_vs_sentiments(date_ticker);

TRUNCATE TABLE target_schema.agg_monthly_stock_prices_vs_sentiments;

INSERT INTO target_schema.agg_monthly_stock_prices_vs_sentiments
SELECT
	CONCAT(DATE_TRUNC('month', date),'-',ticker) AS date_ticker,
	DATE_TRUNC('month', date) AS date,
	ticker,
	AVG(stock_price) AS avg_stock_price,
	ROUND(CAST(AVG(neg) AS DECIMAL),2) AS avg_negative_sentiment,
	ROUND(CAST(AVG(neu) AS DECIMAL),2) AS avg_neutral_sentiment,
	ROUND(CAST(AVG(pos) AS DECIMAL),2) AS avg_positive_sentiment,
	ROUND(CAST(AVG(compound) AS DECIMAL),2) avg_compound_result
FROM target_schema.fact_stock_prices_vs_sentiments
GROUP BY
	DATE_TRUNC('month', date),ticker
ORDER BY
	DATE_TRUNC('month', date);
	
	