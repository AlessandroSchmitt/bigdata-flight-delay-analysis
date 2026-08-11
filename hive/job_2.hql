-- Analysis 3.2 — Delay report by airport and month
--
-- Usage:
-- beeline ... \
--   --hiveconf INPUT=file:///path/to/canonical_input_directory \
--   --hiveconf OUTPUT=file:///path/to/job2_output_directory \
--   -f hive/job_2.hql

DROP TABLE IF EXISTS flights_canonical;

CREATE EXTERNAL TABLE flights_canonical (
    month INT,
    op_unique_carrier STRING,
    origin STRING,
    dep_delay DOUBLE,
    arr_delay DOUBLE,
    cancelled INT,
    cancellation_cause STRING,
    carrier_delay DOUBLE,
    weather_delay DOUBLE,
    nas_delay DOUBLE,
    security_delay DOUBLE,
    late_aircraft_delay DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '${hiveconf:INPUT}'
TBLPROPERTIES ("skip.header.line.count"="1");

WITH
band_stats AS (
    SELECT
        origin,
        month,

        SUM(
            CASE
                WHEN cancelled = 0 AND dep_delay IS NOT NULL AND dep_delay < 15
                THEN 1 ELSE 0
            END
        ) AS low_count,
        ROUND(AVG(
            CASE
                WHEN cancelled = 0 AND dep_delay IS NOT NULL AND dep_delay < 15
                THEN dep_delay
            END
        ), 2) AS low_avg_dep_delay,
        ROUND(AVG(
            CASE
                WHEN cancelled = 0 AND dep_delay IS NOT NULL AND dep_delay < 15
                THEN arr_delay
            END
        ), 2) AS low_avg_arr_delay,

        SUM(
            CASE
                WHEN cancelled = 0 AND dep_delay BETWEEN 15 AND 60
                THEN 1 ELSE 0
            END
        ) AS medium_count,
        ROUND(AVG(
            CASE
                WHEN cancelled = 0 AND dep_delay BETWEEN 15 AND 60
                THEN dep_delay
            END
        ), 2) AS medium_avg_dep_delay,
        ROUND(AVG(
            CASE
                WHEN cancelled = 0 AND dep_delay BETWEEN 15 AND 60
                THEN arr_delay
            END
        ), 2) AS medium_avg_arr_delay,

        SUM(
            CASE
                WHEN cancelled = 0 AND dep_delay IS NOT NULL AND dep_delay > 60
                THEN 1 ELSE 0
            END
        ) AS high_count,
        ROUND(AVG(
            CASE
                WHEN cancelled = 0 AND dep_delay IS NOT NULL AND dep_delay > 60
                THEN dep_delay
            END
        ), 2) AS high_avg_dep_delay,
        ROUND(AVG(
            CASE
                WHEN cancelled = 0 AND dep_delay IS NOT NULL AND dep_delay > 60
                THEN arr_delay
            END
        ), 2) AS high_avg_arr_delay

    FROM flights_canonical
    GROUP BY origin, month
),

cause_incidence AS (
    SELECT
        origin,
        month,
        cause
    FROM flights_canonical
    LATERAL VIEW EXPLODE(
        ARRAY(
            CASE
                WHEN cancelled = 1 AND cancellation_cause = 'CARRIER'
                    THEN 'CARRIER'
                WHEN cancelled = 0 AND carrier_delay > 0
                    THEN 'CARRIER'
            END,
            CASE
                WHEN cancelled = 1 AND cancellation_cause = 'WEATHER'
                    THEN 'WEATHER'
                WHEN cancelled = 0 AND weather_delay > 0
                    THEN 'WEATHER'
            END,
            CASE
                WHEN cancelled = 1 AND cancellation_cause = 'NAS'
                    THEN 'NAS'
                WHEN cancelled = 0 AND nas_delay > 0
                    THEN 'NAS'
            END,
            CASE
                WHEN cancelled = 1 AND cancellation_cause = 'SECURITY'
                    THEN 'SECURITY'
                WHEN cancelled = 0 AND security_delay > 0
                    THEN 'SECURITY'
            END,
            CASE
                WHEN cancelled = 0 AND late_aircraft_delay > 0
                    THEN 'LATE_AIRCRAFT'
            END
        )
    ) exploded AS cause
    WHERE cause IS NOT NULL
),

cause_counts AS (
    SELECT
        origin,
        month,
        cause,
        COUNT(*) AS cause_count
    FROM cause_incidence
    GROUP BY origin, month, cause
),

ranked_causes AS (
    SELECT
        origin,
        month,
        cause,
        cause_count,
        ROW_NUMBER() OVER (
            PARTITION BY origin, month
            ORDER BY cause_count DESC, cause ASC
        ) AS cause_rank
    FROM cause_counts
),

top_causes AS (
    SELECT
        origin,
        month,
        MAX(CASE WHEN cause_rank = 1 THEN cause END) AS top1_cause,
        MAX(CASE WHEN cause_rank = 1 THEN cause_count END) AS top1_count,
        MAX(CASE WHEN cause_rank = 2 THEN cause END) AS top2_cause,
        MAX(CASE WHEN cause_rank = 2 THEN cause_count END) AS top2_count,
        MAX(CASE WHEN cause_rank = 3 THEN cause END) AS top3_cause,
        MAX(CASE WHEN cause_rank = 3 THEN cause_count END) AS top3_count
    FROM ranked_causes
    WHERE cause_rank <= 3
    GROUP BY origin, month
)

INSERT OVERWRITE DIRECTORY '${hiveconf:OUTPUT}'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT
    b.origin AS departure_airport,
    b.month,
    b.low_count,
    b.low_avg_dep_delay,
    b.low_avg_arr_delay,
    b.medium_count,
    b.medium_avg_dep_delay,
    b.medium_avg_arr_delay,
    b.high_count,
    b.high_avg_dep_delay,
    b.high_avg_arr_delay,
    t.top1_cause,
    t.top1_count,
    t.top2_cause,
    t.top2_count,
    t.top3_cause,
    t.top3_count
FROM band_stats b
LEFT JOIN top_causes t
    ON b.origin = t.origin
   AND b.month = t.month;
