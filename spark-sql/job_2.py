#!/usr/bin/env python3
"""Analysis 3.2 implemented with Spark SQL/DataFrame API."""

from __future__ import annotations

import argparse
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import SparkSession, Window, functions as F
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

CAUSE_COLUMNS = [
    ("CARRIER", "carrier_delay"),
    ("WEATHER", "weather_delay"),
    ("NAS", "nas_delay"),
    ("SECURITY", "security_delay"),
    ("LATE_AIRCRAFT", "late_aircraft_delay"),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, help="Canonical input CSV")
    parser.add_argument("--output", required=True, help="Spark output directory")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("analysis-3.2-spark-sql").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        flights = (
            spark.read.option("header", True)
            .schema(SCHEMA)
            .csv(Path(args.input).resolve().as_uri())
            .persist(StorageLevel.MEMORY_AND_DISK)
        )

        eligible = (F.col("cancelled") == 0) & F.col("dep_delay").isNotNull()
        low = eligible & (F.col("dep_delay") < 15)
        medium = eligible & F.col("dep_delay").between(15, 60)
        high = eligible & (F.col("dep_delay") > 60)

        band_stats = (
            flights.groupBy("origin", "month")
            .agg(
                F.sum(F.when(low, 1).otherwise(0)).alias("low_count"),
                F.round(F.avg(F.when(low, F.col("dep_delay"))), 2).alias(
                    "low_avg_dep_delay"
                ),
                F.round(F.avg(F.when(low, F.col("arr_delay"))), 2).alias(
                    "low_avg_arr_delay"
                ),
                F.sum(F.when(medium, 1).otherwise(0)).alias("medium_count"),
                F.round(F.avg(F.when(medium, F.col("dep_delay"))), 2).alias(
                    "medium_avg_dep_delay"
                ),
                F.round(F.avg(F.when(medium, F.col("arr_delay"))), 2).alias(
                    "medium_avg_arr_delay"
                ),
                F.sum(F.when(high, 1).otherwise(0)).alias("high_count"),
                F.round(F.avg(F.when(high, F.col("dep_delay"))), 2).alias(
                    "high_avg_dep_delay"
                ),
                F.round(F.avg(F.when(high, F.col("arr_delay"))), 2).alias(
                    "high_avg_arr_delay"
                ),
            )
        )

        cancelled_causes = flights.select(
            "origin",
            "month",
            F.when(
                (F.col("cancelled") == 1)
                & F.col("cancellation_cause").isNotNull(),
                F.col("cancellation_cause"),
            ).alias("cause"),
        ).filter(F.col("cause").isNotNull())

        delay_cause_array = F.array(
            *[
                F.when(
                    (F.col("cancelled") == 0) & (F.col(column_name) > 0),
                    F.lit(cause_name),
                )
                for cause_name, column_name in CAUSE_COLUMNS
            ]
        )

        delay_causes = (
            flights.select(
                "origin",
                "month",
                F.explode(delay_cause_array).alias("cause"),
            )
            .filter(F.col("cause").isNotNull())
        )

        cause_counts = (
            cancelled_causes.unionByName(delay_causes)
            .groupBy("origin", "month", "cause")
            .count()
            .withColumnRenamed("count", "cause_count")
        )

        rank_window = Window.partitionBy("origin", "month").orderBy(
            F.col("cause_count").desc(),
            F.col("cause").asc(),
        )

        ranked = cause_counts.withColumn(
            "cause_rank",
            F.row_number().over(rank_window),
        ).filter(F.col("cause_rank") <= 3)

        top_causes = ranked.groupBy("origin", "month").agg(
            F.max(F.when(F.col("cause_rank") == 1, F.col("cause"))).alias(
                "top1_cause"
            ),
            F.max(F.when(F.col("cause_rank") == 1, F.col("cause_count"))).alias(
                "top1_count"
            ),
            F.max(F.when(F.col("cause_rank") == 2, F.col("cause"))).alias(
                "top2_cause"
            ),
            F.max(F.when(F.col("cause_rank") == 2, F.col("cause_count"))).alias(
                "top2_count"
            ),
            F.max(F.when(F.col("cause_rank") == 3, F.col("cause"))).alias(
                "top3_cause"
            ),
            F.max(F.when(F.col("cause_rank") == 3, F.col("cause_count"))).alias(
                "top3_count"
            ),
        )

        result = (
            band_stats.join(top_causes, on=["origin", "month"], how="left")
            .select(
                F.col("origin").alias("departure_airport"),
                "month",
                "low_count",
                "low_avg_dep_delay",
                "low_avg_arr_delay",
                "medium_count",
                "medium_avg_dep_delay",
                "medium_avg_arr_delay",
                "high_count",
                "high_avg_dep_delay",
                "high_avg_arr_delay",
                "top1_cause",
                "top1_count",
                "top2_cause",
                "top2_count",
                "top3_cause",
                "top3_count",
            )
        )

        (
            result.write.mode("overwrite")
            .option("header", False)
            .csv(Path(args.output).resolve().as_uri())
        )

        print(f"[OK] Spark SQL Job 2 written to: {Path(args.output).resolve()}")
    finally:
        try:
            flights.unpersist()
        except UnboundLocalError:
            pass
        spark.stop()


if __name__ == "__main__":
    main()
