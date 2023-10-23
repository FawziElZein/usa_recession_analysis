CREATE TABLE IF NOT EXISTS target_schema.agg_quarterly_states_ur(

    state_date TEXT PRIMARY KEY NOT NULL,
    state VARCHAR(2),
    date DATE,
    ur DOUBLE PRECISION,
    ur_trend_value DOUBLE PRECISION,
    ur_trend_percentage NUMERIC
);

CREATE INDEX IF NOT EXISTS agg_quarterly_states_ur_state_date ON target_schema.agg_quarterly_states_ur(state_date);

TRUNCATE TABLE target_schema.agg_quarterly_states_ur;

WITH CTE_AGG_QUARTERLY_STATES_UR AS (
	SELECT
		CONCAT(state,'_',CAST(date_trunc('quarter',date) AS date)) AS state_date,
		state,
		CAST(date_trunc('quarter',date) AS date) AS quarter_start_date,
		ROUND(CAST(AVG(ur) AS DECIMAL),2) AS ur
	FROM target_schema.dim_states_ur
	GROUP BY
		state,date_trunc('quarter',date)
)

INSERT INTO target_schema.agg_quarterly_states_ur
SELECT
	state_date,
	state,
	quarter_start_date AS date,
	ur,
    ROUND(CAST(ur - LAG(ur) OVER (PARTITION BY state ORDER BY quarter_start_date) AS DECIMAL),2) AS ur_trend_value,
	ROUND(CAST((ur - LAG(ur) OVER (PARTITION BY state ORDER BY quarter_start_date))/ABS(LAG(ur) OVER (PARTITION BY state ORDER BY quarter_start_date)) AS DECIMAL),2) AS ur_trend_percentage
FROM CTE_AGG_QUARTERLY_STATES_UR
ORDER BY state,date_trunc('quarter',quarter_start_date);
-- ON CONFLICT(state_date)
-- DO UPDATE SET
--     ur = EXCLUDED.ur,
--     ur_trend_value = EXCLUDED.ur_trend_value,
--     ur_trend_percentage = EXCLUDED.ur_trend_percentage;
