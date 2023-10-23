--- real median household income per state

CREATE TABLE IF NOT EXISTS target_schema.dim_states_mehoin
(
    state_date TEXT PRIMARY KEY NOT NULL,
    state VARCHAR(50),
    date DATE,
    mehoin DOUBLE PRECISION,
    mehoin_trend_value DOUBLE PRECISION,
    mehoin_trend_percentage NUMERIC
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
    mehoin = EXCLUDED.mehoin,
    mehoin_trend_value = EXCLUDED.mehoin_trend_value,
    mehoin_trend_percentage = EXCLUDED.mehoin_trend_percentage;

--- real gross domestic product per state 

CREATE TABLE IF NOT EXISTS target_schema.dim_states_ngsp
(
    state_date TEXT PRIMARY KEY NOT NULL,
    state VARCHAR(50),
    date DATE,
    ngsp DOUBLE PRECISION,
    ngsp_trend_value DOUBLE PRECISION,
    ngsp_trend_percentage NUMERIC
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
    ngsp = EXCLUDED.ngsp,
    ngsp_trend_value = EXCLUDED.ngsp_trend_value,
    ngsp_trend_percentage = EXCLUDED.ngsp_trend_percentage;

--- personal consumption expenditures per state

CREATE TABLE IF NOT EXISTS target_schema.dim_states_pce
(
    state_date TEXT PRIMARY KEY NOT NULL,
    state VARCHAR(50),
    date DATE,
    pce DOUBLE PRECISION,
    pce_trend_value DOUBLE PRECISION,
    pce_trend_percentage NUMERIC
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
    pce = EXCLUDED.pce,
    pce_trend_value = EXCLUDED.pce_trend_value,
    pce_trend_percentage = EXCLUDED.pce_trend_percentage;


--- unemployment rate per state

CREATE TABLE IF NOT EXISTS target_schema.dim_states_ur
(
    state_date TEXT PRIMARY KEY NOT NULL,
    state VARCHAR(50),
    date DATE,
    ur DOUBLE PRECISION,
    ur_trend_value DOUBLE PRECISION,
    ur_trend_percentage NUMERIC
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
    ur = EXCLUDED.ur,
    ur_trend_value = EXCLUDED.ur_trend_value,
    ur_trend_percentage = EXCLUDED.ur_trend_percentage;
