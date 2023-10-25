--- real median household income per state

CREATE TABLE IF NOT EXISTS target_schema.dim_states_mehoin
(
    state_date TEXT PRIMARY KEY NOT NULL,
    state VARCHAR(50),
    date DATE,
    median_household_income DOUBLE PRECISION,
    median_household_income_trend_value DOUBLE PRECISION,
    median_household_income_trend_percentage NUMERIC
);

CREATE INDEX IF NOT EXISTS idx_state_date ON target_schema.dim_states_mehoin(state_date);


INSERT INTO target_schema.dim_states_mehoin
SELECT
    state_date,
    state,
    date,
    mehoin,
	ROUND(CAST(mehoin - LAG(mehoin) OVER (PARTITION BY state ORDER BY date) AS DECIMAL),2) AS mehoin_trend_value,
	ROUND(CAST((mehoin - LAG(mehoin) OVER (PARTITION BY state ORDER BY date))/ABS(LAG(mehoin) OVER (PARTITION BY state ORDER BY date)) AS DECIMAL),2) AS mehoin_trend_percentage
FROM target_schema.stg_fred_economic_data_states_mehoin
ON CONFLICT(state_date)
DO UPDATE SET
    median_household_income = EXCLUDED.median_household_income,
    median_household_income_trend_value = EXCLUDED.median_household_income_trend_value,
    median_household_income_trend_percentage = EXCLUDED.median_household_income_trend_percentage;

--- real gross domestic product per state 

CREATE TABLE IF NOT EXISTS target_schema.dim_states_ngsp
(
    state_date TEXT PRIMARY KEY NOT NULL,
    state VARCHAR(50),
    date DATE,
    gross_domestic_product DOUBLE PRECISION,
    gross_domestic_product_trend_value DOUBLE PRECISION,
    gross_domestic_product_trend_percentage NUMERIC
);

CREATE INDEX IF NOT EXISTS idx_state_date ON target_schema.dim_states_ngsp(state_date);

INSERT INTO target_schema.dim_states_ngsp
SELECT
    state_date,
    state,
    date,
    ngsp,
    ROUND(CAST(ngsp - LAG(ngsp) OVER (PARTITION BY state ORDER BY date) AS DECIMAL),2) AS ngsp_trend_value,
	ROUND(CAST((ngsp - LAG(ngsp) OVER (PARTITION BY state ORDER BY date))/ABS(LAG(ngsp) OVER (PARTITION BY state ORDER BY date)) AS DECIMAL),2) AS ngsp_trend_percentage
FROM target_schema.stg_fred_economic_data_states_ngsp
ON CONFLICT(state_date)
DO UPDATE SET
    gross_domestic_product = EXCLUDED.gross_domestic_product,
    gross_domestic_product_trend_value = EXCLUDED.gross_domestic_product_trend_value,
    gross_domestic_product_trend_percentage = EXCLUDED.gross_domestic_product_trend_percentage;

--- personal consumption expenditures per state

CREATE TABLE IF NOT EXISTS target_schema.dim_states_pce
(
    state_date TEXT PRIMARY KEY NOT NULL,
    state VARCHAR(50),
    date DATE,
    personal_consumption_expenditures DOUBLE PRECISION,
    personal_consumption_expenditures_trend_value DOUBLE PRECISION,
    personal_consumption_expenditures_trend_percentage NUMERIC
);

CREATE INDEX IF NOT EXISTS idx_state_date ON target_schema.dim_states_pce(state_date);

INSERT INTO target_schema.dim_states_pce
SELECT
    state_date,
    state,
    date,
    pce,
    ROUND(CAST(pce - LAG(pce) OVER (PARTITION BY state ORDER BY date) AS DECIMAL),2) AS pce_trend_value,
	ROUND(CAST((pce - LAG(pce) OVER (PARTITION BY state ORDER BY date))/ABS(LAG(pce) OVER (PARTITION BY state ORDER BY date)) AS DECIMAL),2) AS pce_trend_percentage
FROM target_schema.stg_fred_economic_data_states_pce
ON CONFLICT(state_date)
DO UPDATE SET
    personal_consumption_expenditures = EXCLUDED.personal_consumption_expenditures,
    personal_consumption_expenditures_trend_value = EXCLUDED.personal_consumption_expenditures_trend_value,
    personal_consumption_expenditures_trend_percentage = EXCLUDED.personal_consumption_expenditures_trend_percentage;


--- unemployment rate per state

CREATE TABLE IF NOT EXISTS target_schema.dim_states_ur
(
    state_date TEXT PRIMARY KEY NOT NULL,
    state VARCHAR(50),
    date DATE,
    unemployment_rate DOUBLE PRECISION,
    unemployment_rate_trend_value DOUBLE PRECISION,
    unemployment_rate_trend_percentage NUMERIC
);

CREATE INDEX IF NOT EXISTS idx_dim_states_ur_state_date ON target_schema.dim_states_ur(state_date);


INSERT INTO target_schema.dim_states_ur
SELECT
    state_date,
    state,
    date,
    ur,
    ROUND(CAST(ur - LAG(ur) OVER (PARTITION BY state ORDER BY date) AS DECIMAL),2) AS ur_trend_value,
	ROUND(CAST((ur - LAG(ur) OVER (PARTITION BY state ORDER BY date))/ABS(LAG(ur) OVER (PARTITION BY state ORDER BY date)) AS DECIMAL),2) AS ur_trend_percentage
	
FROM target_schema.stg_fred_economic_data_states_ur
ON CONFLICT(state_date)
DO UPDATE SET
    unemployment_rate = EXCLUDED.unemployment_rate,
    unemployment_rate_trend_value = EXCLUDED.unemployment_rate_trend_value,
    unemployment_rate_trend_percentage = EXCLUDED.unemployment_rate_trend_percentage;
