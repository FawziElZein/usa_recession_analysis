CREATE TABLE IF NOT EXISTS target_schema.agg_quarterly_presidentials_speeches_sentiments(

    quarter_start_date DATE PRIMARY KEY NOT NULL,
    number_of_speeches_per_president TEXT,
    average_negative FLOAT,
    average_neutral FLOAT,
    average_positive FLOAT,
    average_compound FLOAT
);

CREATE INDEX IF NOT EXISTS idx_agg_quarterly_presidentials_speeches_sentiments_date ON target_schema.agg_quarterly_presidentials_speeches_sentiments(quarter_start_date);

TRUNCATE TABLE target_schema.agg_quarterly_presidentials_speeches_sentiments;

WITH CTE_QUARTERLY_PRESIDENTS_COUNT AS(
	SELECT
		DATE_TRUNC('quarter', date) AS quarter_start_date,
		speaker_name,
		AVG(neg) AS average_neg,
		AVG(neu) AS average_neu,
		AVG(pos) AS average_pos,
		AVG(compound) AS average_comp,
		COUNT(speech_title) AS total_speeches
	FROM target_schema.fact_presidential_speeches
	GROUP BY DATE_TRUNC('quarter', date),speaker_name
	ORDER BY 
		DATE_TRUNC('quarter', date)
)

INSERT INTO target_schema.agg_quarterly_presidentials_speeches_sentiments
SELECT
    quarter_start_date,
    STRING_AGG(CONCAT(speaker_name,':',total_speeches),'/') AS number_of_speeches_per_president,
    ROUND(CAST(AVG(average_neg) AS DECIMAL),2) AS average_negative,
    ROUND(CAST(AVG(average_neu) AS DECIMAL),2) AS average_neutral,
    ROUND(CAST(AVG(average_pos) AS DECIMAL),2) AS average_positive,
    ROUND(CAST(AVG(average_comp) AS DECIMAL),4) AS average_compound
FROM
    CTE_QUARTERLY_PRESIDENTS_COUNT
GROUP BY
    quarter_start_date;