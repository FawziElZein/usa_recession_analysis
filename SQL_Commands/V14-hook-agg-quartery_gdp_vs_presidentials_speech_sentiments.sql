CREATE TABLE IF NOT EXISTS target_schema.agg_quarterly_gdp_vs_presidentials_speech_sentiments(
    date DATE PRIMARY KEY NOT NULL,
    gdp DOUBLE PRECISION,
    pce DOUBLE PRECISION,
    gpdi DOUBLE PRECISION,
    netexp DOUBLE PRECISION,
    gcec DOUBLE PRECISION,
    impgs DOUBLE PRECISION,
    number_of_speeches_per_president TEXT,
    average_negative DECIMAL,
    average_neutral DECIMAL,
    average_positive DECIMAL,
    average_compound DECIMAL,
    presidents TEXT
);

CREATE INDEX IF NOT EXISTS idx_agg_quarterly_gdp_vs_presidentials_speech_sentiments_date ON target_schema.agg_quarterly_gdp_vs_presidentials_speech_sentiments(date);

TRUNCATE TABLE target_schema.agg_quarterly_gdp_vs_presidentials_speech_sentiments;

INSERT INTO target_schema.agg_quarterly_gdp_vs_presidentials_speech_sentiments(
    SELECT 
        gdp_all_measures.date,
        gdp_all_measures.gdp,
        gdp_all_measures.pce,
        gdp_all_measures.gpdi,
        gdp_all_measures.netexp,
        gdp_all_measures.gcec,
        gdp_all_measures.impgs,
        presidentials_speeches_sentiments.number_of_speeches_per_president,
        presidentials_speeches_sentiments.average_negative,
        presidentials_speeches_sentiments.average_neutral,
        presidentials_speeches_sentiments.average_positive,
        presidentials_speeches_sentiments.average_compound,
        presidentials_speeches_sentiments.presidents
    FROM target_schema.dim_gdp_all_measures AS gdp_all_measures
    INNER JOIN target_schema.agg_quarterly_presidentials_speeches_sentiments AS presidentials_speeches_sentiments
    ON gdp_all_measures.date = presidentials_speeches_sentiments.quarter_start_date
);


