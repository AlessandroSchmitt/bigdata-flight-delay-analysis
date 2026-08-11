#!/usr/bin/env python3
"""Preview and compare Analysis 3.1 outputs.

The script is a validation utility and is not part of benchmark timing.
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)


SCHEMA = StructType(
    [
        StructField("airline", StringType(), False),
        StructField("departure_airport", StringType(), False),
        StructField("flight_count", LongType(), False),
        StructField("min_arr_delay", DoubleType(), True),
        StructField("max_arr_delay", DoubleType(), True),
        StructField("avg_arr_delay", DoubleType(), True),
        StructField("cancellation_rate", DoubleType(), False),
        StructField("operating_months", StringType(), False),
    ]
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--left", required=True, help="First output directory")
    parser.add_argument("--right", help="Optional second output directory")
    parser.add_argument("--show", type=int, default=10)
    parser.add_argument("--tolerance", type=float, default=1e-9)
    return parser.parse_args()


def read_result(spark: SparkSession, path: str):
    return spark.read.schema(SCHEMA).option("header", False).csv(path)


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("analysis-3.1-check").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        left = read_result(spark, args.left)

        print(f"[INFO] Left rows: {left.count()}")
        print("[INFO] First rows (deterministically sorted for preview only):")
        left.orderBy("airline", "departure_airport").show(
            args.show, truncate=False
        )

        duplicate_keys = (
            left.groupBy("airline", "departure_airport")
            .count()
            .filter(F.col("count") != 1)
            .count()
        )
        print(f"[INFO] Left duplicate keys: {duplicate_keys}")

        if not args.right:
            return

        right = read_result(spark, args.right)
        print(f"[INFO] Right rows: {right.count()}")

        l = left.alias("l")
        r = right.alias("r")

        joined = l.join(
            r,
            on=["airline", "departure_airport"],
            how="full_outer",
        )

        def numeric_mismatch(name: str):
            lc = F.col(f"l.{name}")
            rc = F.col(f"r.{name}")
            return (
                (lc.isNull() & rc.isNotNull())
                | (lc.isNotNull() & rc.isNull())
                | (
                    lc.isNotNull()
                    & rc.isNotNull()
                    & (F.abs(lc - rc) > F.lit(args.tolerance))
                )
            )

        mismatch = joined.filter(
            F.col("l.flight_count").isNull()
            | F.col("r.flight_count").isNull()
            | (F.col("l.flight_count") != F.col("r.flight_count"))
            | numeric_mismatch("min_arr_delay")
            | numeric_mismatch("max_arr_delay")
            | numeric_mismatch("avg_arr_delay")
            | numeric_mismatch("cancellation_rate")
            | (
                F.coalesce(F.col("l.operating_months"), F.lit(""))
                != F.coalesce(F.col("r.operating_months"), F.lit(""))
            )
        )

        mismatch_count = mismatch.count()
        print(f"[INFO] Mismatching keys: {mismatch_count}")

        if mismatch_count:
            mismatch.select(
                "airline",
                "departure_airport",
                "l.flight_count",
                "r.flight_count",
                "l.min_arr_delay",
                "r.min_arr_delay",
                "l.max_arr_delay",
                "r.max_arr_delay",
                "l.avg_arr_delay",
                "r.avg_arr_delay",
                "l.cancellation_rate",
                "r.cancellation_rate",
                "l.operating_months",
                "r.operating_months",
            ).show(20, truncate=False)
        else:
            print("[OK] Outputs are equivalent within the configured tolerance.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
