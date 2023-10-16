CREATE TABLE IF NOT EXISTS target_schema.agg_daily_fang_price
(
    ticker_formatted_date TEXT PRIMARY KEY NOT NULL,
    ticker VARCHAR(100),
    formatted_date DATE,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    open DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    adjclose DOUBLE PRECISION
);

CREATE INDEX IF NOT EXISTS idx_agg_daily_fang_price_ticker_formatted_date ON target_schema.agg_daily_fang_price(ticker_formatted_date);

TRUNCATE TABLE target_schema.agg_daily_fang_price;

INSERT INTO target_schema.agg_daily_fang_price
SELECT
	ticker_formatted_date,
    ticker,
    formatted_date,
    high,
    low,
    open,
    close,
    volume,
    adjclose
FROM target_schema.dim_faang_stock_price; 