CREATE TABLE IF NOT EXISTS target_schema.stg_finviz_financial_news(

    news_id SERIAL PRIMARY KEY NOT NULL,
    ticker VARCHAR(100),
    date DATE,
    time TIME,
    title TEXT,
    text TEXT,
    url TEXT
);

CREATE INDEX IF NOT EXISTS idx_news_id ON target_schema.stg_finviz_financial_news(news_id);


--- USA real government consumption expenditures and gross investment
CREATE TABLE IF NOT EXISTS target_schema.stg_fred_economic_data_gcec1(
    date,
    gcec1
);

CREATE INDEX IF NOT EXISTS idx_date ON target_schema.stg_fred_economic_data_gcec1(date);

--- USA real gross domestic product

CREATE TABLE IF NOT EXISTS target_schema.stg_fred_economic_data_gdpc1(
    date,
    gdpc1
);

CREATE INDEX IF NOT EXISTS idx_date ON target_schema.stg_fred_economic_data_gdpc1(date);

--- USA gross private domestic investment

CREATE TABLE IF NOT EXISTS target_schema.stg_fred_economic_data_gpdi(
    date,
    gpdi
);

CREATE INDEX IF NOT EXISTS idx_date ON target_schema.stg_fred_economic_data_gpdi(date);

--- USA imports of goods and services

CREATE TABLE IF NOT EXISTS target_schema.stg_fred_economic_data_impgs(
    date,
    impgs
);

CREATE INDEX IF NOT EXISTS idx_date ON target_schema.stg_fred_economic_data_impgs(date);

--- net exports of goods and services

CREATE TABLE IF NOT EXISTS target_schema.stg_fred_economic_data_netexp(
    date,
    netexp
);

CREATE INDEX IF NOT EXISTS idx_date ON target_schema.stg_fred_economic_data_netexp(date);

--- USA product consumption expenditure 

CREATE TABLE IF NOT EXISTS target_schema.stg_fred_economic_data_pce(
    date,
    pce
);
CREATE INDEX IF NOT EXISTS idx_date ON target_schema.stg_fred_economic_data_pce(date);


CREATE TABLE IF NOT EXISTS target_schema.stg_fred_economic_data_states_ngsp(
    state_date TEXT PRIMARY KEY NOT NULL,
    state VARCHAR(100),
    date DATE,
    ngsp FLOAT
);

CREATE INDEX IF NOT EXISTS idx_state_date ON target_schema.stg_fred_economic_data_states_ngsp(state_date);

CREATE TABLE IF NOT EXISTS target_schema.stg_fred_economic_data_states_ur(
    state_date TEXT PRIMARY KEY NOT NULL,
    state VARCHAR(100),
    date DATE,
    ur FLOAT
);
CREATE INDEX IF NOT EXISTS idx_state_date ON target_schema.stg_fred_economic_data_states_ur(state_date);

CREATE TABLE IF NOT EXISTS target_schema.stg_fred_economic_data_states_mehoin(
    state_date TEXT PRIMARY KEY NOT NULL,
    state VARCHAR(100),
    date DATE,
    mehoin FLOAT
);
CREATE INDEX IF NOT EXISTS idx_state_date ON target_schema.stg_fred_economic_data_states_mehoin(state_date);

CREATE TABLE IF NOT EXISTS target_schema.stg_fred_economic_data_states_pce(
    state_date TEXT PRIMARY KEY NOT NULL,
    state VARCHAR(100),
    date DATE,
    pce FLOAT
);
CREATE INDEX IF NOT EXISTS idx_state_date ON target_schema.stg_fred_economic_data_states_pce(state_date);


-- APPLE stock price staging table

CREATE TABLE IF NOT EXISTS target_schema.stg_yahoo_finance_aapl_stock_price(

    formatted_date DATE PRIMARY KEY NOT NULL,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    open DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    adjclose DOUBLE PRECISION
);

CREATE INDEX IF NOT EXISTS idx_formatted_date ON target_schema.stg_yahoo_finance_aapl_stock_price(formatted_date);

-- AMAZON stock price staging table

CREATE TABLE IF NOT EXISTS target_schema.stg_yahoo_finance_amzn_stock_price(
    formatted_date DATE PRIMARY KEY NOT NULL,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    open DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    adjclose DOUBLE PRECISION
);


CREATE INDEX IF NOT EXISTS idx_formatted_date ON target_schema.stg_yahoo_finance_amzn_stock_price(formatted_date);

-- GOOGLE stock price staging table

CREATE TABLE IF NOT EXISTS target_schema.stg_yahoo_finance_googl_stock_price(
    formatted_date DATE PRIMARY KEY NOT NULL,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    open DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    adjclose DOUBLE PRECISION
);

CREATE INDEX IF NOT EXISTS idx_formatted_date ON target_schema.stg_yahoo_finance_googl_stock_price(formatted_date);


-- META stock price staging table

CREATE TABLE IF NOT EXISTS target_schema.stg_meta_finance_meta_stock_price(
    formatted_date DATE PRIMARY KEY NOT NULL,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    open DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    adjclose DOUBLE PRECISION
);

CREATE INDEX IF NOT EXISTS idx_formatted_date ON target_schema.stg_meta_finance_meta_stock_price(formatted_date);


-- NETFLIX staging table

CREATE TABLE IF NOT EXISTS target_schema.stg_meta_finance_nflx_stock_price(
    formatted_date DATE PRIMARY KEY NOT NULL,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    open DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    adjclose DOUBLE PRECISION
);

CREATE INDEX IF NOT EXISTS idx_formatted_date ON target_schema.stg_meta_finance_nflx_stock_price(formatted_date);



--- Presidential speeches staging table

CREATE TABLE IF NOT EXISTS target_schema.stg_miller_center_presidential_speeches(
    date DATE PRIMARY KEY NOT NULL,
    speech_title TEXT,
    speaker_name VARCHAR(200),
    speech TEXT
);

CREATE INDEX IF NOT EXISTS idx_date ON target_schema.stg_miller_center_presidential_speeches(date);
