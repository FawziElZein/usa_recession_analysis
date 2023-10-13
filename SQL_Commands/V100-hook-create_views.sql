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
    FROM dw_reporting.fact_stock_prices_vs_sentiments
    ORDER BY 
        ticker,date,time;


    