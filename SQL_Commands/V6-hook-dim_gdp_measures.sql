--- real gross domestic product
CREATE TABLE IF NOT EXISTS target_schema.dim_real_gross_domestic_product(

    date DATE PRIMARY KEY NOT NULL,
    gdp_value FLOAT
);

CREATE INDEX IF NOT EXISTS idx_date ON target_schema.dim_real_gross_domestic_product(date);

INSERT INTO target_schema.dim_real_gross_domestic_product
SELECT
    date,
    gdpc1
FROM target_schema.stg_fred_economic_data_gdpc1;



--- personal consumption expenditures
CREATE TABLE IF NOT EXISTS target_schema.dim_personal_consumption_expenditures(
    
    date DATE PRIMARY KEY NOT NULL,
    pce_value FLOAT
);

CREATE INDEX IF NOT EXISTS idx_date ON target_schema.dim_personal_consumption_expenditures(date);

INSERT INTO target_schema.dim_personal_consumption_expenditures
SELECT 
    CAST(date_trunc('quarter', date) AS DATE) AS quarter_start,
	SUM(pce) AS pce_quarterly_value
FROM target_schema.stg_fred_economic_data_pce
GROUP BY 
	date_trunc('quarter', date)
ORDER BY
	date_trunc('quarter', date);


--- gross private domestic investment
CREATE TABLE IF NOT EXISTS target_schema.dim_gross_private_domestic_investment(
    
    date DATE PRIMARY KEY NOT NULL,
    gpdi_value FLOAT
);

CREATE INDEX IF NOT EXISTS idx_date ON target_schema.dim_gross_private_domestic_investment(date);

INSERT INTO target_schema.dim_gross_private_domestic_investment
SELECT
    date,
    gpdi
FROM target_schema.stg_fred_economic_data_gpdi;


-- net exports of goods and services
CREATE TABLE IF NOT EXISTS target_schema.dim_net_exports_of_goods_and_services(
    
    date DATE PRIMARY KEY NOT NULL,
    netexp_value FLOAT
);

CREATE INDEX IF NOT EXISTS idx_date ON target_schema.dim_net_exports_of_goods_and_services(date);

INSERT INTO target_schema.dim_net_exports_of_goods_and_services
SELECT
    date,
    netexp
FROM target_schema.stg_fred_economic_data_netexp;

--- real government consumption expenditures and gross investment
CREATE TABLE IF NOT EXISTS target_schema.dim_real_government_consumption_expenditures_and_gross_invest(
    
    date DATE PRIMARY KEY NOT NULL,
    gcec1_value FLOAT
);

CREATE INDEX IF NOT EXISTS idx_date ON target_schema.dim_real_government_consumption_expenditures_and_gross_invest(date);

INSERT INTO target_schema.dim_real_government_consumption_expenditures_and_gross_invest
SELECT
    date,
    gcec1
FROM target_schema.stg_fred_economic_data_gcec1;

--- imports of goods and services
CREATE TABLE IF NOT EXISTS target_schema.dim_imports_of_goods_and_services(
    
    date DATE PRIMARY KEY NOT NULL,
    impgs_value FLOAT
);

CREATE INDEX IF NOT EXISTS idx_date ON target_schema.dim_imports_of_goods_and_services(date);

INSERT INTO target_schema.dim_imports_of_goods_and_services
SELECT
    date,
    impgs
FROM target_schema.stg_fred_economic_data_impgs;


CREATE TABLE IF NOT EXISTS target_schema.dim_gdp_all_measures(

    date DATE PRIMARY KEY NOT NULL,
    gdp FLOAT,
    pce FLOAT,
    gpdi FLOAT,
    netexp FLOAT,
    gcec FLOAT,
    impgs FLOAT
);

CREATE INDEX IF NOT EXISTS idx_date ON target_schema.dim_gdp_all_measures(date);

WITH CTE_PCE_PER_QUARTER AS(

SELECT 
    CAST(date_trunc('quarter', date) AS DATE) AS quarter_start,
	SUM(pce) AS pce
FROM dw_reporting.stg_fred_economic_data_pce
GROUP BY 
	date_trunc('quarter', date)
ORDER BY
	date_trunc('quarter', date)
)

INSERT INTO target_schema.dim_gdp_all_measures
SELECT
	stg_gdp.date,
	stg_gdp.gdpc1,
	CTE_PCE_PER_QUARTER.pce,
	stg_gpdi.gpdi,
	stg_netexp.netexp,
	stg_gcec1.gcec1,
	stg_impgs.impgs
FROM target_schema.stg_fred_economic_data_gdpc1 AS stg_gdp
INNER JOIN CTE_PCE_PER_QUARTER
ON stg_gdp.date = CTE_PCE_PER_QUARTER.quarter_start
INNER JOIN target_schema.stg_fred_economic_data_gpdi AS stg_gpdi
ON stg_gdp.date = stg_gpdi.date
INNER JOIN target_schema.stg_fred_economic_data_netexp AS stg_netexp
ON stg_gdp.date = stg_netexp.date
INNER JOIN target_schema.stg_fred_economic_data_gcec1 AS stg_gcec1
ON stg_gdp.date = stg_gcec1.date
INNER JOIN target_schema.stg_fred_economic_data_impgs AS stg_impgs
ON stg_gdp.date = stg_impgs.date;