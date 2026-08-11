#!/usr/bin/env python3
"""Analysis 3.1 implemented with Spark SQL/DataFrame API."""

from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import SparkSession, functions as F
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
    spark = SparkSession.builder.appName("analysis-3.1-spark-sql").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        flights = (
            spark.read.option("header", True)
            .schema(SCHEMA)
            .csv(Path(args.input).resolve().as_uri())
        )

        result = (
            flights.groupBy("op_unique_carrier", "origin")
            .agg(
                F.count(F.lit(1)).alias("flight_count"),
                F.min("arr_delay").alias("min_arr_delay"),
                F.max("arr_delay").alias("max_arr_delay"),
                F.round(F.avg("arr_delay"), 2).alias("avg_arr_delay"),
                F.round(
                    F.sum(F.col("cancelled")) / F.count(F.lit(1)),
                    4,
                ).alias("cancellation_rate"),
                F.sort_array(
                    F.collect_set(
                        F.lpad(F.col("month").cast("string"), 2, "0")
                    )
                ).alias("months"),
            )
            .select(
                F.col("op_unique_carrier").alias("airline"),
                F.col("origin").alias("departure_airport"),
                "flight_count",
                "min_arr_delay",
                "max_arr_delay",
                "avg_arr_delay",
                "cancellation_rate",
                F.concat_ws("|", F.col("months")).alias("operating_months"),
            )
        )

        (
            result.write.mode("overwrite")
            .option("header", False)
            .csv(Path(args.output).resolve().as_uri())
        )

        print(f"[OK] Spark SQL Job 1 written to: {Path(args.output).resolve()}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
