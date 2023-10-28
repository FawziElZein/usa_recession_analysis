--- FAANG stock prices view
DROP VIEW IF EXISTS target_schema.vw_stock_prices;

CREATE VIEW target_schema.vw_stock_prices AS
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
    FROM target_schema.agg_daily_fang_price;

-- FAANG stock prices daily trend view

DROP VIEW IF EXISTS target_schema.vw_stock_prices_trend;

CREATE VIEW target_schema.vw_stock_prices_trend AS
    SELECT
        ticker_formatted_date,
        ticker,
        formatted_date,
        avg_day_price,
        price_trend_value,
        price_trend_percentage
    FROM target_schema.fact_faang_stock_prices_trend;


--- FAANG stock prices vs news sentiments view

DROP VIEW IF EXISTS target_schema.vw_stock_prices_vs_sentiments;

CREATE VIEW target_schema.vw_stock_prices_vs_sentiments AS
    SELECT
        ticker_title,
        ticker,
        title,
        date,
        time,
        stock_price,
        neg,
        neu,
        pos,
        compound,
        url,
        text
    FROM target_schema.fact_stock_prices_vs_sentiments
    ORDER BY 
        ticker,date,time;

--- Daily stock prices vs sentiments view
DROP VIEW IF EXISTS target_schema.vw_daily_stock_prices_vs_sentiments;

CREATE VIEW target_schema.vw_daily_stock_prices_vs_sentiments AS
    SELECT
        date_ticker,
        date,
        ticker,
        stock_price,
        neg,
        neu,
        pos,
        compound
    FROM target_schema.agg_daily_stock_prices_vs_sentiments
    ORDER BY 
        date_ticker;


--- Monthly stock prices vs sentiments view

DROP VIEW IF EXISTS target_schema.vw_monthly_stock_prices_vs_sentiments;

CREATE VIEW target_schema.vw_monthly_stock_prices_vs_sentiments AS
    SELECT
        date_ticker,
        date,
        ticker,
        stock_price,
        neg,
        neu,
        pos,
        compound
    FROM target_schema.agg_monthly_stock_prices_vs_sentiments;

--- United states GDP view

DROP VIEW IF EXISTS target_schema.vw_gross_domestic_product_all_measures;

CREATE VIEW target_schema.vw_gross_domestic_product_all_measures AS
    SELECT
    date,
    gross_domestic_product,
    gross_domestic_product_trend_value,
    gross_domestic_product_trend_percentage,
    personal_consumption_expenditures,
    personal_consumption_expenditures_trend_value,
    personal_consumption_expenditures_trend_percentage,
    gross_private_domestic_investment,
    gross_private_domestic_investment_trend_value,
    gross_private_domestic_investment_trend_percentage,
    net_exports_of_goods_and_services,
    net_exports_of_goods_and_services_trend_value,
    net_exports_of_goods_and_services_trend_percentage,
    government_consumption_expenditures,
    government_consumption_expenditures_trend_value,
    government_consumption_expenditures_trend_percentage,
    imports_of_goods_and_services,
    imports_of_goods_and_services_trend_value,
    imports_of_goods_and_services_trend_percentage
    FROM target_schema.dim_gross_domestic_product_all_measures;

--- GDP of all states in U.S

DROP VIEW IF EXISTS target_schema.vw_all_states_gross_domestic_product;

CREATE VIEW target_schema.vw_all_states_gross_domestic_product AS
    SELECT
        state_date,
        state,
        target_schema.get_state_name(state) AS state_name,
        date,
        gross_domestic_product,
        gross_domestic_product_trend_value,
        gross_domestic_product_trend_percentage
    FROM target_schema.dim_states_gross_domestic_product;

-- PCE of all states in U.S

DROP VIEW IF EXISTS target_schema.vw_all_states_personal_consumption_expenditures;

CREATE VIEW target_schema.vw_all_states_personal_consumption_expenditures AS
    SELECT
        state_date,
        state,
        target_schema.get_state_name(state) AS state_name,
        date,
        personal_consumption_expenditures,
        personal_consumption_expenditures_trend_value,
        personal_consumption_expenditures_trend_percentage
    FROM target_schema.dim_states_personal_consumption_expenditures;

--- MEHOIN of all states in U.S.

DROP VIEW IF EXISTS target_schema.vw_all_states_median_household_income;

CREATE VIEW target_schema.vw_all_states_median_household_income AS
    SELECT
        state_date,
        state,
        target_schema.get_state_name(state) AS state_name,
        date,
        median_household_income,
        median_household_income_trend_value,
        median_household_income_trend_percentage
    FROM target_schema.dim_states_median_household_income;


--- Unemployment rate of all states in the US per month

DROP VIEW IF EXISTS target_schema.vw_all_states_monthly_unemployment_rate;

CREATE VIEW target_schema.vw_all_states_monthly_unemployment_rate AS
    SELECT
        state_date,
        state,
        target_schema.get_state_name(state) AS state_name,
        date,
        unemployment_rate,
        unemployment_rate_trend_value,
        unemployment_rate_trend_percentage
    FROM target_schema.dim_states_unemployment_rate;

--- Unemployment rate of all states in the US per quarter

DROP VIEW IF EXISTS target_schema.vw_all_states_quarterly_unemployment_rate;

CREATE VIEW target_schema.vw_all_states_quarterly_unemployment_rate AS
    SELECT
        state_date,
        state,
        target_schema.get_state_name(state) AS state_name,
        date,
        unemployment_rate,
        unemployment_rate_trend_value,
        unemployment_rate_trend_percentage
    FROM target_schema.agg_quarterly_states_unemployment_rate;

--- Unemployment rate of all states in the US per year

DROP VIEW IF EXISTS target_schema.vw_all_states_yearly_unemployment_rate;

CREATE VIEW target_schema.vw_all_states_yearly_unemployment_rate AS
    SELECT
        state_date,
        state,
        target_schema.get_state_name(state) AS state_name,
        date,
        unemployment_rate,
        unemployment_rate_trend_value,
        unemployment_rate_trend_percentage
    FROM target_schema.agg_yearly_states_unemployment_rate;

--- GDP vs Presidentials speech sentiments 
DROP VIEW IF EXISTS target_schema.vw_gross_domestic_product_vs_presidentials_speech_sentiments;

CREATE VIEW target_schema.vw_gross_domestic_product_vs_presidentials_speech_sentiments AS
    SELECT
        date,
        gross_domestic_product,
        personal_consumption_expenditures,
        gross_private_domestic_investment,
        net_exports_of_goods_and_services,
        government_consumption_expenditures,
        imports_of_goods_and_services,
        presidents,
        number_of_speeches_per_president,
        average_negative,
        average_neutral,
        average_positive,
        average_compound
    FROM target_schema.agg_quarterly_gdp_vs_presidentials_speech_sentiments;


