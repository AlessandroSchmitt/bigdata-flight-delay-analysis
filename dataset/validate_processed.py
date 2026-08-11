#!/usr/bin/env python3
"""Lightweight validation for the canonical processed CSV."""

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
    "cause_type",
    "cause",
]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("flight-data-validation").getOrCreate()
    try:
        df = spark.read.option("header", True).option("inferSchema", True).csv(
            Path(args.input).resolve().as_uri()
        )

        if df.columns != EXPECTED:
            raise AssertionError(f"Unexpected columns: {df.columns}")

        invalid = df.filter(
            (~F.col("month").between(1, 12))
            | (~F.col("cancelled").isin(0, 1))
            | F.col("op_unique_carrier").isNull()
            | (F.length(F.trim(F.col("op_unique_carrier"))) == 0)
            | F.col("origin").isNull()
            | (F.length(F.trim(F.col("origin"))) == 0)
            | (
                F.col("cause").isNotNull()
                & ~F.col("cause").isin(
                    "CARRIER", "WEATHER", "NAS", "SECURITY", "LATE_AIRCRAFT"
                )
            )
            | (
                F.col("cause_type").isNotNull()
                & ~F.col("cause_type").isin("CANCELLATION", "DELAY")
            )
        )

        invalid_count = invalid.count()
        if invalid_count:
            raise AssertionError(f"Found {invalid_count} rows violating the data contract")

        print(f"[OK] Validation passed. Rows: {df.count()}")
        df.printSchema()
        df.show(10, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
