--- real median household income per state

CREATE TABLE IF NOT EXISTS target_schema.dim_states_mehoin
(
    state_date TEXT PRIMARY KEY NOT NULL,
    state VARCHAR(50),
    date DATE,
    mehoin DOUBLE PRECISION
);

CREATE INDEX IF NOT EXISTS idx_state_date ON target_schema.dim_states_mehoin(state_date);

TRUNCATE TABLE target_schema.dim_states_mehoin;

INSERT INTO target_schema.dim_states_mehoin
SELECT
    state_date,
    state,
    date,
    mehoin
FROM target_schema.stg_fred_economic_data_states_mehoin;

--- real gross domestic product per state 

CREATE TABLE IF NOT EXISTS target_schema.dim_states_ngsp
(
    state_date TEXT PRIMARY KEY NOT NULL,
    state VARCHAR(50),
    date DATE,
    ngsp DOUBLE PRECISION
);

CREATE INDEX IF NOT EXISTS idx_state_date ON target_schema.dim_states_ngsp(state_date);

TRUNCATE TABLE target_schema.dim_states_ngsp;

INSERT INTO target_schema.dim_states_ngsp
SELECT
    state_date,
    state,
    date,
    ngsp
FROM target_schema.stg_fred_economic_data_states_ngsp;

--- personal consumption expenditures per state

CREATE TABLE IF NOT EXISTS target_schema.dim_states_pce
(
    state_date TEXT PRIMARY KEY NOT NULL,
    state VARCHAR(50),
    date DATE,
    pce DOUBLE PRECISION
);

CREATE INDEX IF NOT EXISTS idx_state_date ON target_schema.dim_states_pce(state_date);

TRUNCATE TABLE target_schema.dim_states_pce;

INSERT INTO target_schema.dim_states_pce
SELECT
    state_date,
    state,
    date,
    pce
FROM target_schema.stg_fred_economic_data_states_pce;


--- unemployment rate per state

CREATE TABLE IF NOT EXISTS target_schema.dim_states_ur
(
    state_date TEXT PRIMARY KEY NOT NULL,
    state VARCHAR(50),
    date DATE,
    ur DOUBLE PRECISION
);

CREATE INDEX IF NOT EXISTS idx_state_date ON target_schema.dim_states_ur(state_date);

TRUNCATE TABLE target_schema.dim_states_ur;

INSERT INTO target_schema.dim_states_ur
SELECT
    state_date,
    state,
    date,
    ur
FROM target_schema.stg_fred_economic_data_states_ur;