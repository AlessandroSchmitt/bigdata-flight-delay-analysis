#!/usr/bin/env python3
"""Analysis 3.1 implemented with explicit Spark SQL."""

from __future__ import annotations

import argparse
from pathlib import Path

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
        .appName("analysis-3.1-spark-sql")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        flights = (
            spark.read
            .option("header", True)
            .schema(SCHEMA)
            .csv(args.input if "://" in args.input else Path(args.input).resolve().as_uri())
        )

        flights.createOrReplaceTempView("flights")

        result = spark.sql(
            """
            SELECT
                op_unique_carrier AS airline,
                origin AS departure_airport,
                COUNT(*) AS flight_count,
                MIN(arr_delay) AS min_arr_delay,
                MAX(arr_delay) AS max_arr_delay,
                ROUND(AVG(arr_delay), 2) AS avg_arr_delay,
                ROUND(
                    CAST(SUM(cancelled) AS DOUBLE) / COUNT(*),
                    4
                ) AS cancellation_rate,
                CONCAT_WS(
                    '|',
                    SORT_ARRAY(
                        COLLECT_SET(
                            LPAD(CAST(month AS STRING), 2, '0')
                        )
                    )
                ) AS operating_months
            FROM flights
            GROUP BY op_unique_carrier, origin
            """
        )

        (
            result.write
            .mode("overwrite")
            .option("header", False)
            .csv(args.output if "://" in args.output else Path(args.output).resolve().as_uri())
        )

        print(
            f"[OK] Spark SQL Job 1 written to: "
            f"{Path(args.output).resolve()}"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
