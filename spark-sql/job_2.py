#!/usr/bin/env python3
"""Analysis 3.2 implemented with explicit Spark SQL."""

from __future__ import annotations

import argparse
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


SCHEMA = StructType(
    [
        StructField("month", IntegerType(), True),
        StructField("op_unique_carrier", StringType(), True),
        StructField("origin", StringType(), True),
        StructField("dep_delay", DoubleType(), True),
        StructField("arr_delay", DoubleType(), True),
        StructField("cancelled", IntegerType(), True),
        StructField("cancellation_cause", StringType(), True),
        StructField("carrier_delay", DoubleType(), True),
        StructField("weather_delay", DoubleType(), True),
        StructField("nas_delay", DoubleType(), True),
        StructField("security_delay", DoubleType(), True),
        StructField("late_aircraft_delay", DoubleType(), True),
    ]
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, help="Canonical input CSV")
    parser.add_argument("--output", required=True, help="Spark output directory")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("analysis-3.2-spark-sql")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    flights = None

    try:
        flights = (
            spark.read
            .option("header", True)
            .schema(SCHEMA)
            .csv(Path(args.input).resolve().as_uri())
            .persist(StorageLevel.MEMORY_AND_DISK)
        )

        # The same canonical relation is referenced by two analytical branches
        # in the SQL query. Persisting it avoids reparsing the CSV independently.
        flights.createOrReplaceTempView("flights")

        result = spark.sql(
            """
            WITH
            band_stats AS (
                SELECT
                    origin,
                    month,

                    SUM(
                        CASE
                            WHEN cancelled = 0
                             AND dep_delay IS NOT NULL
                             AND dep_delay < 15
                            THEN 1 ELSE 0
                        END
                    ) AS low_count,
                    ROUND(
                        AVG(
                            CASE
                                WHEN cancelled = 0
                                 AND dep_delay IS NOT NULL
                                 AND dep_delay < 15
                                THEN dep_delay
                            END
                        ),
                        2
                    ) AS low_avg_dep_delay,
                    ROUND(
                        AVG(
                            CASE
                                WHEN cancelled = 0
                                 AND dep_delay IS NOT NULL
                                 AND dep_delay < 15
                                THEN arr_delay
                            END
                        ),
                        2
                    ) AS low_avg_arr_delay,

                    SUM(
                        CASE
                            WHEN cancelled = 0
                             AND dep_delay BETWEEN 15 AND 60
                            THEN 1 ELSE 0
                        END
                    ) AS medium_count,
                    ROUND(
                        AVG(
                            CASE
                                WHEN cancelled = 0
                                 AND dep_delay BETWEEN 15 AND 60
                                THEN dep_delay
                            END
                        ),
                        2
                    ) AS medium_avg_dep_delay,
                    ROUND(
                        AVG(
                            CASE
                                WHEN cancelled = 0
                                 AND dep_delay BETWEEN 15 AND 60
                                THEN arr_delay
                            END
                        ),
                        2
                    ) AS medium_avg_arr_delay,

                    SUM(
                        CASE
                            WHEN cancelled = 0
                             AND dep_delay IS NOT NULL
                             AND dep_delay > 60
                            THEN 1 ELSE 0
                        END
                    ) AS high_count,
                    ROUND(
                        AVG(
                            CASE
                                WHEN cancelled = 0
                                 AND dep_delay IS NOT NULL
                                 AND dep_delay > 60
                                THEN dep_delay
                            END
                        ),
                        2
                    ) AS high_avg_dep_delay,
                    ROUND(
                        AVG(
                            CASE
                                WHEN cancelled = 0
                                 AND dep_delay IS NOT NULL
                                 AND dep_delay > 60
                                THEN arr_delay
                            END
                        ),
                        2
                    ) AS high_avg_arr_delay

                FROM flights
                GROUP BY origin, month
            ),

            cause_incidence AS (
                SELECT
                    origin,
                    month,
                    cause
                FROM flights
                LATERAL VIEW EXPLODE(
                    ARRAY(
                        CASE
                            WHEN cancelled = 1
                             AND cancellation_cause = 'CARRIER'
                                THEN 'CARRIER'
                            WHEN cancelled = 0
                             AND carrier_delay > 0
                                THEN 'CARRIER'
                        END,
                        CASE
                            WHEN cancelled = 1
                             AND cancellation_cause = 'WEATHER'
                                THEN 'WEATHER'
                            WHEN cancelled = 0
                             AND weather_delay > 0
                                THEN 'WEATHER'
                        END,
                        CASE
                            WHEN cancelled = 1
                             AND cancellation_cause = 'NAS'
                                THEN 'NAS'
                            WHEN cancelled = 0
                             AND nas_delay > 0
                                THEN 'NAS'
                        END,
                        CASE
                            WHEN cancelled = 1
                             AND cancellation_cause = 'SECURITY'
                                THEN 'SECURITY'
                            WHEN cancelled = 0
                             AND security_delay > 0
                                THEN 'SECURITY'
                        END,
                        CASE
                            WHEN cancelled = 0
                             AND late_aircraft_delay > 0
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
                    MAX(
                        CASE WHEN cause_rank = 1 THEN cause END
                    ) AS top1_cause,
                    MAX(
                        CASE WHEN cause_rank = 1 THEN cause_count END
                    ) AS top1_count,
                    MAX(
                        CASE WHEN cause_rank = 2 THEN cause END
                    ) AS top2_cause,
                    MAX(
                        CASE WHEN cause_rank = 2 THEN cause_count END
                    ) AS top2_count,
                    MAX(
                        CASE WHEN cause_rank = 3 THEN cause END
                    ) AS top3_cause,
                    MAX(
                        CASE WHEN cause_rank = 3 THEN cause_count END
                    ) AS top3_count
                FROM ranked_causes
                WHERE cause_rank <= 3
                GROUP BY origin, month
            )

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
             AND b.month = t.month
            """
        )

        (
            result.write
            .mode("overwrite")
            .option("header", False)
            .csv(Path(args.output).resolve().as_uri())
        )

        print(
            f"[OK] Spark SQL Job 2 written to: "
            f"{Path(args.output).resolve()}"
        )
    finally:
        if flights is not None:
            flights.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()
