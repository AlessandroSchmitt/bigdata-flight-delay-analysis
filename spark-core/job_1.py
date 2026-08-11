#!/usr/bin/env python3
"""Analysis 3.1 implemented with Spark Core RDDs."""

from __future__ import annotations

import argparse
import csv
from decimal import Decimal, ROUND_HALF_UP
from io import StringIO
from pathlib import Path
from typing import Iterable, Iterator, Optional, Tuple

from pyspark import SparkConf, SparkContext


Accumulator = Tuple[
    int,                 # flight_count
    int,                 # valid_arrival_delay_count
    float,               # arrival_delay_sum
    Optional[float],     # arrival_delay_min
    Optional[float],     # arrival_delay_max
    int,                 # cancelled_count
    frozenset[int],      # months
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, help="Canonical input CSV")
    parser.add_argument("--output", required=True, help="Spark output directory")
    return parser.parse_args()


def parse_csv_line(line: str) -> list[str]:
    return next(csv.reader(StringIO(line)))


def parse_partition(
    partition_index: int, lines: Iterable[str]
) -> Iterator[Tuple[Tuple[str, str], Accumulator]]:
    iterator = iter(lines)

    # The canonical input is one CSV file with exactly one header line.
    if partition_index == 0:
        next(iterator, None)

    for line in iterator:
        row = parse_csv_line(line)
        if len(row) < 12:
            raise ValueError(f"Malformed canonical row with {len(row)} columns")

        month = int(row[0])
        carrier = row[1]
        origin = row[2]
        arr_delay = float(row[4]) if row[4] != "" else None
        cancelled = int(row[5])

        valid_count = 1 if arr_delay is not None else 0
        arr_sum = arr_delay if arr_delay is not None else 0.0

        accumulator: Accumulator = (
            1,
            valid_count,
            arr_sum,
            arr_delay,
            arr_delay,
            cancelled,
            frozenset((month,)),
        )

        yield (carrier, origin), accumulator


def min_optional(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if a is None:
        return b
    if b is None:
        return a
    return min(a, b)


def max_optional(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if a is None:
        return b
    if b is None:
        return a
    return max(a, b)


def merge_accumulators(a: Accumulator, b: Accumulator) -> Accumulator:
    return (
        a[0] + b[0],
        a[1] + b[1],
        a[2] + b[2],
        min_optional(a[3], b[3]),
        max_optional(a[4], b[4]),
        a[5] + b[5],
        a[6] | b[6],
    )


def format_optional(value: Optional[float]) -> str:
    return "" if value is None else str(value)

def round_half_up(value: float, digits: int) -> float:
    """Match the HALF_UP rounding semantics used by Spark SQL/Hive ROUND."""
    quantum = Decimal("1").scaleb(-digits)
    return float(
        Decimal(str(value)).quantize(
            quantum,
            rounding=ROUND_HALF_UP
        )
    )


def format_result(
    item: Tuple[Tuple[str, str], Accumulator]
) -> str:
    (carrier, origin), stats = item
    (
        flight_count,
        valid_delay_count,
        delay_sum,
        delay_min,
        delay_max,
        cancelled_count,
        months,
    ) = stats

    avg_delay = (
    round_half_up(delay_sum / valid_delay_count, 2)
    if valid_delay_count > 0
    else None
    )
    cancellation_rate = round_half_up(
    cancelled_count / flight_count,
    4
    )
    operating_months = "|".join(f"{month:02d}" for month in sorted(months))

    fields = [
        carrier,
        origin,
        str(flight_count),
        format_optional(delay_min),
        format_optional(delay_max),
        format_optional(avg_delay),
        str(cancellation_rate),
        operating_months,
    ]

    buffer = StringIO()
    csv.writer(buffer, lineterminator="").writerow(fields)
    return buffer.getvalue()


def main() -> None:
    args = parse_args()

    conf = SparkConf().setAppName("analysis-3.1-spark-core")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")

    try:
        input_uri = Path(args.input).resolve().as_uri()
        output_uri = Path(args.output).resolve().as_uri()

        lines = sc.textFile(input_uri)

        stats = (
            lines.mapPartitionsWithIndex(parse_partition)
            .reduceByKey(merge_accumulators)
            .map(format_result)
        )

        stats.saveAsTextFile(output_uri)

        print(f"[OK] Spark Core Job 1 written to: {Path(args.output).resolve()}")
    finally:
        sc.stop()


if __name__ == "__main__":
    main()
