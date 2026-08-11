#!/usr/bin/env python3
"""Validate the canonical processed Flight Delay CSV."""

from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


EXPECTED = [
    "month",
    "op_unique_carrier",
    "origin",
    "dep_delay",
    "arr_delay",
    "cancelled",
    "cancellation_cause",
    "carrier_delay",
    "weather_delay",
    "nas_delay",
    "security_delay",
    "late_aircraft_delay",
]

DELAY_COLUMNS = [
    "carrier_delay",
    "weather_delay",
    "nas_delay",
    "security_delay",
    "late_aircraft_delay",
]

ALLOWED_CAUSES = ["CARRIER", "WEATHER", "NAS", "SECURITY"]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("flight-data-validation").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        df = (
            spark.read.option("header", True)
            .option("inferSchema", True)
            .csv(Path(args.input).resolve().as_uri())
        )

        if df.columns != EXPECTED:
            raise AssertionError(f"Unexpected columns: {df.columns}")

        invalid_condition = (
            (~F.col("month").between(1, 12))
            | (~F.col("cancelled").isin(0, 1))
            | F.col("op_unique_carrier").isNull()
            | (F.length(F.trim(F.col("op_unique_carrier"))) == 0)
            | F.col("origin").isNull()
            | (F.length(F.trim(F.col("origin"))) == 0)
            | (
                F.col("cancellation_cause").isNotNull()
                & ~F.col("cancellation_cause").isin(*ALLOWED_CAUSES)
            )
            | (
                (F.col("cancelled") == 0)
                & F.col("cancellation_cause").isNotNull()
            )
        )

        for name in DELAY_COLUMNS:
            invalid_condition = invalid_condition | F.col(name).isNull() | (F.col(name) < 0)

        invalid_count = df.filter(invalid_condition).count()
        if invalid_count:
            raise AssertionError(f"Found {invalid_count} rows violating the data contract")

        row_count = df.count()
        print(f"[OK] Validation passed. Rows: {row_count}")
        df.printSchema()
        df.show(10, truncate=False)

        positive_cause_count = sum(
            [F.when(F.col(c) > 0, 1).otherwise(0) for c in DELAY_COLUMNS]
        )

        print("\n[INFO] Positive delay-cause categories per non-cancelled flight:")
        (
            df.filter(F.col("cancelled") == 0)
            .withColumn("positive_delay_causes", positive_cause_count)
            .groupBy("positive_delay_causes")
            .count()
            .orderBy("positive_delay_causes")
            .show()
        )

        cancelled_without_cause = df.filter(
            (F.col("cancelled") == 1) & F.col("cancellation_cause").isNull()
        ).count()
        print(
            "[INFO] Cancelled flights without a recognized cancellation cause:",
            cancelled_without_cause,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
