CREATE TABLE IF NOT EXISTS target_schema.fact_faang_stock_prices_trend(
	ticker_formatted_date TEXT PRIMARY KEY NOT NULL,
	ticker VARCHAR(6),
	formatted_date DATE,
	avg_day_price DECIMAL,
	price_trend_value DECIMAL,
	price_trend_percentage DECIMAL
);

CREATE INDEX IF NOT EXISTS idx_fact_faang_stock_prices_trend_ticker_formatted_date ON target_schema.fact_faang_stock_prices_trend(ticker_formatted_date);

TRUNCATE TABLE target_schema.fact_faang_stock_prices_trend;

WITH CTE_DAILY_STOCK_PRICE AS (
SELECT
	ticker_formatted_date,
	ticker,
	formatted_date AS date,
	ROUND(CAST ((high + low)/2 AS DECIMAL),2) AS avg_day_price
FROM dw_reporting.dim_faang_stock_price
)

INSERT INTO target_schema.fact_faang_stock_prices_trend
SELECT
	ticker_formatted_date,
	ticker,
	date,
	avg_day_price,
	ROUND(avg_day_price - LAG(avg_day_price) OVER (PARTITION BY ticker ORDER BY date),2) AS price_trend_value,
	ROUND((avg_day_price - LAG(avg_day_price) OVER (PARTITION BY ticker ORDER BY date))/ABS(LAG(avg_day_price) OVER (PARTITION BY ticker ORDER BY date)),3)*100 AS price_trend_percentage
FROM CTE_DAILY_STOCK_PRICE
ORDER BY date,ticker;
