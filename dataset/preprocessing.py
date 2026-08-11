#!/usr/bin/env python3
"""Create the canonical cleaned Flight Delay 2024 dataset.

The script keeps data-preparation semantics independent from the analytical
technology (Hive, Spark Core, Spark SQL) that will consume the result.
"""

from __future__ import annotations

import argparse
import os
import shutil
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType


REQUIRED_COLUMNS = {
    "month",
    "op_unique_carrier",
    "origin",
    "dep_delay",
    "arr_delay",
    "cancelled",
    "cancellation_code",
    "diverted",
    "carrier_delay",
    "weather_delay",
    "nas_delay",
    "security_delay",
    "late_aircraft_delay",
}

DELAY_COLUMNS = [
    "carrier_delay",
    "weather_delay",
    "nas_delay",
    "security_delay",
    "late_aircraft_delay",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, help="Path to flight_data_2024.csv")
    parser.add_argument(
        "--output",
        required=True,
        help="Destination CSV file, e.g. data/processed/flights_cleaned.csv",
    )
    return parser.parse_args()


def require_columns(df: DataFrame) -> None:
    missing = sorted(REQUIRED_COLUMNS.difference(df.columns))
    if missing:
        raise ValueError(f"Missing required input columns: {', '.join(missing)}")


def normalize_base_columns(df: DataFrame) -> DataFrame:
    """Cast/normalize fields while preserving null delays and negative values."""
    out = (
        df.select(*sorted(REQUIRED_COLUMNS))
        .withColumn("month", F.col("month").cast(IntegerType()))
        .withColumn("cancelled", F.col("cancelled").cast(IntegerType()))
        .withColumn("diverted", F.col("diverted").cast(IntegerType()))
        .withColumn("dep_delay", F.col("dep_delay").cast(DoubleType()))
        .withColumn("arr_delay", F.col("arr_delay").cast(DoubleType()))
        .withColumn("op_unique_carrier", F.upper(F.trim(F.col("op_unique_carrier"))))
        .withColumn("origin", F.upper(F.trim(F.col("origin"))))
        .withColumn("cancellation_code", F.upper(F.trim(F.col("cancellation_code"))))
    )

    # Cause-delay columns represent minutes attributed to each category.
    # Missing values are normalized to zero so all three technologies consume
    # the same explicit representation.
    for name in DELAY_COLUMNS:
        out = out.withColumn(
            name,
            F.coalesce(F.col(name).cast(DoubleType()), F.lit(0.0)),
        )

    return out


def filter_invalid_rows(df: DataFrame) -> DataFrame:
    """Apply only row-level rules already fixed in the data contract."""
    return df.filter(
        (F.col("diverted") == 0)
        & F.col("month").between(1, 12)
        & F.col("cancelled").isin(0, 1)
        & F.col("op_unique_carrier").isNotNull()
        & (F.length(F.col("op_unique_carrier")) > 0)
        & F.col("origin").isNotNull()
        & (F.length(F.col("origin")) > 0)
    )


def add_cancellation_cause(df: DataFrame) -> DataFrame:
    """Normalize cancellation codes while preserving all delay-cause columns."""
    normalized = (
        F.when(F.col("cancellation_code") == "A", F.lit("CARRIER"))
        .when(F.col("cancellation_code") == "B", F.lit("WEATHER"))
        .when(F.col("cancellation_code") == "C", F.lit("NAS"))
        .when(F.col("cancellation_code") == "D", F.lit("SECURITY"))
        .otherwise(F.lit(None).cast("string"))
    )

    return df.withColumn(
        "cancellation_cause",
        F.when(F.col("cancelled") == 1, normalized).otherwise(
            F.lit(None).cast("string")
        ),
    )


def build_canonical_dataset(raw: DataFrame) -> DataFrame:
    require_columns(raw)
    clean = normalize_base_columns(raw)
    clean = filter_invalid_rows(clean)
    clean = add_cancellation_cause(clean)

    return clean.select(
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
    )


def write_single_csv(df: DataFrame, output_file: str) -> None:
    """Write one canonical CSV file.

    The single-file consolidation is deliberate during preprocessing only. It
    simplifies identical ingestion across Hive, Spark Core and Spark SQL. The
    preprocessing cost is kept separate from analytical benchmark timings.
    """
    output = Path(output_file).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    temp_dir = output.parent / f".{output.stem}_spark_tmp"

    if temp_dir.exists():
        shutil.rmtree(temp_dir)
    if output.exists():
        output.unlink()

    (
        df.coalesce(1)
        .write.mode("overwrite")
        .option("header", True)
        .csv(temp_dir.as_uri())
    )

    part_files = list(temp_dir.glob("part-*.csv"))
    if len(part_files) != 1:
        raise RuntimeError(f"Expected one CSV part file, found {len(part_files)}")

    os.replace(part_files[0], output)
    shutil.rmtree(temp_dir)


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("flight-data-preprocessing").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        raw = (
            spark.read.option("header", True)
            .option("inferSchema", True)
            .csv(Path(args.input).resolve().as_uri())
        )

        canonical = build_canonical_dataset(raw)
        write_single_csv(canonical, args.output)

        print(f"[OK] Canonical dataset written to: {Path(args.output).resolve()}")
        print("[INFO] Columns:", ", ".join(canonical.columns))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
