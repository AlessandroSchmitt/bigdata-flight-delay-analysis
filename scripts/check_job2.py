#!/usr/bin/env python3
"""Preview and compare Analysis 3.2 outputs.

The script is a validation utility and is not part of benchmark timing.
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


SCHEMA = StructType(
    [
        StructField("departure_airport", StringType(), False),
        StructField("month", IntegerType(), False),
        StructField("low_count", LongType(), False),
        StructField("low_avg_dep_delay", DoubleType(), True),
        StructField("low_avg_arr_delay", DoubleType(), True),
        StructField("medium_count", LongType(), False),
        StructField("medium_avg_dep_delay", DoubleType(), True),
        StructField("medium_avg_arr_delay", DoubleType(), True),
        StructField("high_count", LongType(), False),
        StructField("high_avg_dep_delay", DoubleType(), True),
        StructField("high_avg_arr_delay", DoubleType(), True),
        StructField("top1_cause", StringType(), True),
        StructField("top1_count", LongType(), True),
        StructField("top2_cause", StringType(), True),
        StructField("top2_count", LongType(), True),
        StructField("top3_cause", StringType(), True),
        StructField("top3_count", LongType(), True),
    ]
)

COUNT_COLUMNS = [
    "low_count",
    "medium_count",
    "high_count",
    "top1_count",
    "top2_count",
    "top3_count",
]

FLOAT_COLUMNS = [
    "low_avg_dep_delay",
    "low_avg_arr_delay",
    "medium_avg_dep_delay",
    "medium_avg_arr_delay",
    "high_avg_dep_delay",
    "high_avg_arr_delay",
]

STRING_COLUMNS = ["top1_cause", "top2_cause", "top3_cause"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--left", required=True)
    parser.add_argument("--right")
    parser.add_argument("--show", type=int, default=10)
    parser.add_argument("--tolerance", type=float, default=1e-9)
    return parser.parse_args()


def read_result(spark: SparkSession, path: str):
    df = spark.read.schema(SCHEMA).option("header", False).csv(path)

    # Hive TextFile output serializes null STRING values as the literal "\\N".
    # Spark CSV output uses empty fields for nulls. Normalize both to logical
    # nulls before comparing implementations.
    for name in STRING_COLUMNS:
        df = df.withColumn(
            name,
            F.when(F.col(name) == r"\N", F.lit(None).cast("string"))
            .otherwise(F.col(name))
        )

    return df


def null_safe_exact(left, right):
    return (
        (left.isNull() & right.isNull())
        | (left.isNotNull() & right.isNotNull() & (left == right))
    )


def numeric_equal(left, right, tolerance: float):
    return (
        (left.isNull() & right.isNull())
        | (
            left.isNotNull()
            & right.isNotNull()
            & (F.abs(left - right) <= F.lit(tolerance))
        )
    )


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("analysis-3.2-check").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        left = read_result(spark, args.left)

        print(f"[INFO] Left rows: {left.count()}")
        print("[INFO] First rows (deterministically sorted for preview only):")
        left.orderBy("departure_airport", "month").show(
            args.show, truncate=False
        )

        duplicate_keys = (
            left.groupBy("departure_airport", "month")
            .count()
            .filter(F.col("count") != 1)
            .count()
        )
        print(f"[INFO] Left duplicate keys: {duplicate_keys}")

        if not args.right:
            return

        right = read_result(spark, args.right)
        print(f"[INFO] Right rows: {right.count()}")

        joined = left.alias("l").join(
            right.alias("r"),
            on=["departure_airport", "month"],
            how="full_outer",
        )

        mismatch_condition = (
            F.col("l.low_count").isNull()
            | F.col("r.low_count").isNull()
        )

        for name in COUNT_COLUMNS:
            mismatch_condition = mismatch_condition | ~null_safe_exact(
                F.col(f"l.{name}"), F.col(f"r.{name}")
            )

        for name in FLOAT_COLUMNS:
            mismatch_condition = mismatch_condition | ~numeric_equal(
                F.col(f"l.{name}"),
                F.col(f"r.{name}"),
                args.tolerance,
            )

        for name in STRING_COLUMNS:
            mismatch_condition = mismatch_condition | ~null_safe_exact(
                F.col(f"l.{name}"), F.col(f"r.{name}")
            )

        mismatch = joined.filter(mismatch_condition)
        mismatch_count = mismatch.count()

        print(f"[INFO] Mismatching keys: {mismatch_count}")

        if mismatch_count:
            mismatch.show(20, truncate=False)
        else:
            print("[OK] Outputs are equivalent within the configured tolerance.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
