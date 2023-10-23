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
    gdp_trend_value DOUBLE PRECISION,
    gdp_trend_percentage NUMERIC,
    pce FLOAT,
    pce_trend_value DOUBLE PRECISION,
    pce_trend_percentage NUMERIC,
    gpdi FLOAT,
    gpdi_trend_value DOUBLE PRECISION,
    gpdi_trend_percentage NUMERIC,
    netexp FLOAT,
    netexp_trend_value DOUBLE PRECISION,
    netexp_trend_percentage NUMERIC,
    gcec FLOAT,
    gcec_trend_value DOUBLE PRECISION,
    gcec_trend_percentage NUMERIC,
    impgs FLOAT,
    impgs_trend_value DOUBLE PRECISION,
    impgs_trend_percentage NUMERIC
);

CREATE INDEX IF NOT EXISTS idx_date ON target_schema.dim_gdp_all_measures(date);

WITH CTE_PCE_PER_QUARTER AS(

SELECT 
    CAST(date_trunc('quarter', date) AS DATE) AS quarter_start,
	SUM(pce) AS pce
FROM target_schema.stg_fred_economic_data_pce
GROUP BY 
	date_trunc('quarter', date)
ORDER BY
	date_trunc('quarter', date)
)

INSERT INTO target_schema.dim_gdp_all_measures
SELECT
	stg_gdp.date,
	stg_gdp.gdpc1,
    ROUND(CAST(stg_gdp.gdpc1 - LAG(stg_gdp.gdpc1) OVER (ORDER BY stg_gdp.date) AS DECIMAL),2) AS gdp_trend_value,
    ROUND(CAST((stg_gdp.gdpc1 - LAG(stg_gdp.gdpc1) OVER (ORDER BY stg_gdp.date))/ABS(LAG(stg_gdp.gdpc1) OVER (ORDER BY stg_gdp.date)) AS DECIMAL),4)*100  AS gdp_trend_percentage,
	CTE_PCE_PER_QUARTER.pce,
    ROUND(CAST(CTE_PCE_PER_QUARTER.pce - LAG(CTE_PCE_PER_QUARTER.pce) OVER (ORDER BY stg_gdp.date) AS DECIMAL),2) AS pce_trend_value,
    ROUND(CAST((CTE_PCE_PER_QUARTER.pce - LAG(CTE_PCE_PER_QUARTER.pce) OVER (ORDER BY stg_gdp.date))/ABS(LAG(CTE_PCE_PER_QUARTER.pce) OVER (ORDER BY stg_gdp.date)) AS DECIMAL),4)*100  AS pce_trend_percentage,
	stg_gpdi.gpdi,
    ROUND(CAST(stg_gpdi.gpdi - LAG(stg_gpdi.gpdi) OVER (ORDER BY stg_gdp.date) AS DECIMAL),2)AS gpdi_trend_value,
    ROUND(CAST((stg_gpdi.gpdi - LAG(stg_gpdi.gpdi) OVER (ORDER BY stg_gdp.date))/ABS(LAG(stg_gpdi.gpdi) OVER (ORDER BY stg_gdp.date)) AS DECIMAL),4)*100  AS gpdi_trend_percentage,
	stg_netexp.netexp,
    ROUND(CAST(stg_netexp.netexp - LAG(stg_netexp.netexp) OVER (ORDER BY stg_gdp.date) AS DECIMAL),2) AS netexp_trend_value,
    ROUND(CAST((stg_netexp.netexp - LAG(stg_netexp.netexp) OVER (ORDER BY stg_gdp.date))/ABS(LAG(stg_netexp.netexp) OVER (ORDER BY stg_gdp.date)) AS DECIMAL),4)*100  AS netexp_trend_percentage,
	stg_gcec1.gcec1,
    ROUND(CAST(stg_gcec1.gcec1 - LAG(stg_gcec1.gcec1) OVER (ORDER BY stg_gdp.date) AS DECIMAL),2) AS gcec1_trend_value,
    ROUND(CAST((stg_gcec1.gcec1 - LAG(stg_gcec1.gcec1) OVER (ORDER BY stg_gdp.date))/ABS(LAG(stg_gcec1.gcec1) OVER (ORDER BY stg_gdp.date)) AS DECIMAL),4)*100  AS gcec1_trend_percentage,
	stg_impgs.impgs,
    ROUND(CAST(stg_impgs.impgs - LAG(stg_impgs.impgs) OVER (ORDER BY stg_gdp.date) AS DECIMAL),2) AS impgs_trend_value,
    ROUND(CAST((stg_impgs.impgs - LAG(stg_impgs.impgs) OVER (ORDER BY stg_gdp.date))/ABS(LAG(stg_impgs.impgs) OVER (ORDER BY stg_gdp.date)) AS DECIMAL),4)*100  AS impgs_trend_percentage
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