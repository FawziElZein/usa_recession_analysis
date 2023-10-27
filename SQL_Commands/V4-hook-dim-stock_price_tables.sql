--- APPLE STOCK MARKET TABLE ---
CREATE TABLE IF NOT EXISTS target_schema.dim_appl_stock_price(
    formatted_date DATE PRIMARY KEY NOT NULL,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    open DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    adjclose DOUBLE PRECISION
);

CREATE INDEX IF NOT EXISTS idx_dim_appl_stock_price_formatted_date ON target_schema.dim_appl_stock_price(formatted_date);

INSERT INTO target_schema.dim_appl_stock_price
(
    SELECT
        formatted_date,
        high,
        low,
        open,
        close,
        volume,
        adjclose
    FROM target_schema.stg_yahoo_finance_aapl_stock_price
)
ON CONFLICT(formatted_date)
DO UPDATE SET
    formatted_date = EXCLUDED.formatted_date,
    high = EXCLUDED.high,
    low = EXCLUDED.low,
    open = EXCLUDED.open,
    close = EXCLUDED.close,
    volume = EXCLUDED.volume,
    adjclose = EXCLUDED.adjclose;


--- AMAZON STOCK MARKET TABLE ---

CREATE TABLE IF NOT EXISTS target_schema.dim_amzn_stock_price(
    formatted_date DATE PRIMARY KEY NOT NULL,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    open DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    adjclose DOUBLE PRECISION
);

CREATE INDEX IF NOT EXISTS idx_dim_amzn_stock_price_formatted_date ON target_schema.dim_amzn_stock_price(formatted_date);

INSERT INTO target_schema.dim_amzn_stock_price
(
    SELECT
        formatted_date,
        high,
        low,
        open,
        close,
        volume,
        adjclose
    FROM target_schema.stg_yahoo_finance_amzn_stock_price
)
ON CONFLICT(formatted_date)
DO UPDATE SET
    formatted_date = EXCLUDED.formatted_date,
    high = EXCLUDED.high,
    low = EXCLUDED.low,
    open = EXCLUDED.open,
    close = EXCLUDED.close,
    volume = EXCLUDED.volume,
    adjclose = EXCLUDED.adjclose;


--- GOOGLE STOCK MARKET TABLE ---

CREATE TABLE IF NOT EXISTS target_schema.dim_googl_stock_price(
    formatted_date DATE PRIMARY KEY NOT NULL,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    open DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    adjclose DOUBLE PRECISION
);

CREATE INDEX IF NOT EXISTS idx_dim_googl_stock_price_formatted_date ON target_schema.dim_googl_stock_price(formatted_date);

INSERT INTO target_schema.dim_googl_stock_price
(
    SELECT
        formatted_date,
        high,
        low,
        open,
        close,
        volume,
        adjclose
    FROM target_schema.stg_yahoo_finance_googl_stock_price
)
ON CONFLICT(formatted_date)
DO UPDATE SET
    formatted_date = EXCLUDED.formatted_date,
    high = EXCLUDED.high,
    low = EXCLUDED.low,
    open = EXCLUDED.open,
    close = EXCLUDED.close,
    volume = EXCLUDED.volume,
    adjclose = EXCLUDED.adjclose;



--- META STOCK MARKET TABLE ---

CREATE TABLE IF NOT EXISTS target_schema.dim_meta_stock_price(
    formatted_date DATE PRIMARY KEY NOT NULL,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    open DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    adjclose DOUBLE PRECISION
);

CREATE INDEX IF NOT EXISTS idx_dim_meta_stock_price_formatted_date ON target_schema.dim_meta_stock_price(formatted_date);

INSERT INTO target_schema.dim_meta_stock_price
(
    SELECT
        formatted_date,
        high,
        low,
        open,
        close,
        volume,
        adjclose
    FROM target_schema.stg_yahoo_finance_meta_stock_price
)
ON CONFLICT(formatted_date)
DO UPDATE SET
    formatted_date = EXCLUDED.formatted_date,
    high = EXCLUDED.high,
    low = EXCLUDED.low,
    open = EXCLUDED.open,
    close = EXCLUDED.close,
    volume = EXCLUDED.volume,
    adjclose = EXCLUDED.adjclose;


--- NETFLIX STOCK MARKET TABLE ---

CREATE TABLE IF NOT EXISTS target_schema.dim_nflx_stock_price(
    formatted_date DATE PRIMARY KEY NOT NULL,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    open DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    adjclose DOUBLE PRECISION
);

CREATE INDEX IF NOT EXISTS idx_dim_nflx_stock_price_formatted_date ON target_schema.dim_nflx_stock_price(formatted_date);

INSERT INTO target_schema.dim_nflx_stock_price
(
    SELECT
        formatted_date,
        high,
        low,
        open,
        close,
        volume,
        adjclose
    FROM target_schema.stg_yahoo_finance_nflx_stock_price
)
ON CONFLICT(formatted_date)
DO UPDATE SET
    formatted_date = EXCLUDED.formatted_date,
    high = EXCLUDED.high,
    low = EXCLUDED.low,
    open = EXCLUDED.open,
    close = EXCLUDED.close,
    volume = EXCLUDED.volume,
    adjclose = EXCLUDED.adjclose;
    
--- FAANG STOCK MARKET TABLE ---

CREATE TABLE IF NOT EXISTS target_schema.dim_faang_stock_price(

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

CREATE INDEX IF NOT EXISTS idx_dim_faang_stock_price_ticker_formatted_date ON target_schema.dim_faang_stock_price(ticker_formatted_date);

INSERT INTO target_schema.dim_faang_stock_price
(


    SELECT
        CONCAT('AAPL','-',formatted_date) AS ticker_formatted_date,
        'AAPL' AS ticker,
        formatted_date,
        high,
        low,
        open,
        close,
        volume,
        adjclose
    FROM target_schema.stg_yahoo_finance_aapl_stock_price
    UNION
    SELECT
        CONCAT('AMZN','-',formatted_date) AS ticker_formatted_date,
        'AMZN' AS ticker,
        formatted_date,
        high,
        low,
        open,
        close,
        volume,
        adjclose
    FROM target_schema.stg_yahoo_finance_amzn_stock_price
    UNION
    SELECT
        CONCAT('GOOGL','-',formatted_date) AS ticker_formatted_date,
        'GOOGL' AS ticker,
        formatted_date,
        high,
        low,
        open,
        close,
        volume,
        adjclose
    FROM target_schema.stg_yahoo_finance_googl_stock_price
    UNION
    SELECT
        CONCAT('META','-',formatted_date) AS ticker_formatted_date,
        'META' AS ticker,
        formatted_date,
        high,
        low,
        open,
        close,
        volume,
        adjclose
    FROM target_schema.stg_yahoo_finance_meta_stock_price 
    UNION
    SELECT
        CONCAT('NFLX','-',formatted_date) AS ticker_formatted_date,
        'NFLX' AS ticker,
        formatted_date,
        high,
        low,
        open,
        close,
        volume,
        adjclose
    FROM target_schema.stg_yahoo_finance_nflx_stock_price

ORDER BY
	ticker_formatted_date
)
ON CONFLICT(ticker_formatted_date)
DO UPDATE SET
    high = EXCLUDED.high,
    low = EXCLUDED.low,
    open = EXCLUDED.open,
    close = EXCLUDED.close,
    volume = EXCLUDED.volume,
    adjclose = EXCLUDED.adjclose;

