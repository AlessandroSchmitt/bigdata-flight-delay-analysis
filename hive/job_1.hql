-- Analysis 3.1 — Airline statistics
--
-- Usage example:
-- beeline ... \
--   --hiveconf INPUT=/path/to/flights_cleaned.csv \
--   --hiveconf OUTPUT=/path/to/job1_hive_output \
--   -f hive/job_1.hql

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

INSERT OVERWRITE DIRECTORY '${hiveconf:OUTPUT}'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT
    op_unique_carrier AS airline,
    origin AS departure_airport,
    COUNT(*) AS flight_count,
    MIN(arr_delay) AS min_arr_delay,
    MAX(arr_delay) AS max_arr_delay,
    ROUND(AVG(arr_delay), 2) AS avg_arr_delay,
    ROUND(SUM(cancelled) / COUNT(*), 4) AS cancellation_rate,
    CONCAT_WS(
        '|',
        SORT_ARRAY(
            COLLECT_SET(LPAD(CAST(month AS STRING), 2, '0'))
        )
    ) AS operating_months
FROM flights_canonical
GROUP BY op_unique_carrier, origin;
