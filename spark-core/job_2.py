#!/usr/bin/env python3
"""Analysis 3.2 implemented with Spark Core RDDs."""

from __future__ import annotations

import argparse
import csv
from decimal import Decimal, ROUND_HALF_UP
from io import StringIO
from pathlib import Path
from typing import Iterable, Iterator, Optional, Tuple

from pyspark import SparkConf, SparkContext, StorageLevel


BandAccumulator = Tuple[
    int, float, int, float,  # LOW: count, dep sum, arr valid count, arr sum
    int, float, int, float,  # MEDIUM
    int, float, int, float,  # HIGH
]

CAUSE_FIELDS = [
    ("CARRIER", 7),
    ("WEATHER", 8),
    ("NAS", 9),
    ("SECURITY", 10),
    ("LATE_AIRCRAFT", 11),
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
) -> Iterator[tuple]:
    iterator = iter(lines)
    if partition_index == 0:
        next(iterator, None)

    for line in iterator:
        row = parse_csv_line(line)
        if len(row) < 12:
            raise ValueError(f"Malformed canonical row with {len(row)} columns")

        month = int(row[0])
        origin = row[2]
        dep_delay = float(row[3]) if row[3] != "" else None
        arr_delay = float(row[4]) if row[4] != "" else None
        cancelled = int(row[5])
        cancellation_cause = row[6] if row[6] != "" else None
        delay_minutes = tuple(float(row[index]) for _, index in CAUSE_FIELDS)

        yield (
            origin,
            month,
            dep_delay,
            arr_delay,
            cancelled,
            cancellation_cause,
            delay_minutes,
        )


def empty_band_accumulator() -> BandAccumulator:
    return (0, 0.0, 0, 0.0, 0, 0.0, 0, 0.0, 0, 0.0, 0, 0.0)


def record_to_band(item: tuple) -> Tuple[Tuple[str, int], BandAccumulator]:
    origin, month, dep_delay, arr_delay, cancelled, _, _ = item

    values = list(empty_band_accumulator())

    if cancelled == 0 and dep_delay is not None:
        if dep_delay < 15:
            offset = 0
        elif dep_delay <= 60:
            offset = 4
        else:
            offset = 8

        values[offset] = 1
        values[offset + 1] = dep_delay

        if arr_delay is not None:
            values[offset + 2] = 1
            values[offset + 3] = arr_delay

    return (origin, month), tuple(values)


def merge_band(a: BandAccumulator, b: BandAccumulator) -> BandAccumulator:
    return tuple(x + y for x, y in zip(a, b))  # type: ignore[return-value]


def record_to_causes(
    item: tuple,
) -> Iterator[Tuple[Tuple[str, int, str], int]]:
    origin, month, _, _, cancelled, cancellation_cause, delay_minutes = item

    if cancelled == 1:
        if cancellation_cause is not None:
            yield (origin, month, cancellation_cause), 1
        return

    for (cause_name, _), minutes in zip(CAUSE_FIELDS, delay_minutes):
        if minutes > 0:
            yield (origin, month, cause_name), 1


def keep_top3(values: Iterable[Tuple[str, int]]) -> Tuple[Tuple[str, int], ...]:
    return tuple(sorted(values, key=lambda x: (-x[1], x[0]))[:3])


def create_top3(value: Tuple[str, int]) -> Tuple[Tuple[str, int], ...]:
    return (value,)


def merge_top3_value(
    current: Tuple[Tuple[str, int], ...],
    value: Tuple[str, int],
) -> Tuple[Tuple[str, int], ...]:
    return keep_top3((*current, value))


def merge_top3_combiner(
    left: Tuple[Tuple[str, int], ...],
    right: Tuple[Tuple[str, int], ...],
) -> Tuple[Tuple[str, int], ...]:
    return keep_top3((*left, *right))


def round_half_up(value: float, digits: int) -> float:
    quantum = Decimal("1").scaleb(-digits)
    return float(
        Decimal(str(value)).quantize(quantum, rounding=ROUND_HALF_UP)
    )


def average_or_none(total: float, count: int) -> Optional[float]:
    if count == 0:
        return None
    return round_half_up(total / count, 2)


def format_optional(value: object) -> str:
    return "" if value is None else str(value)


def format_result(
    item: Tuple[
        Tuple[str, int],
        Tuple[BandAccumulator, Optional[Tuple[Tuple[str, int], ...]]],
    ]
) -> str:
    (origin, month), (band, causes) = item

    low_count, low_dep_sum, low_arr_count, low_arr_sum = band[0:4]
    med_count, med_dep_sum, med_arr_count, med_arr_sum = band[4:8]
    high_count, high_dep_sum, high_arr_count, high_arr_sum = band[8:12]

    top = list(causes or ())
    while len(top) < 3:
        top.append((None, None))  # type: ignore[arg-type]

    fields = [
        origin,
        str(month),
        str(low_count),
        format_optional(average_or_none(low_dep_sum, low_count)),
        format_optional(average_or_none(low_arr_sum, low_arr_count)),
        str(med_count),
        format_optional(average_or_none(med_dep_sum, med_count)),
        format_optional(average_or_none(med_arr_sum, med_arr_count)),
        str(high_count),
        format_optional(average_or_none(high_dep_sum, high_count)),
        format_optional(average_or_none(high_arr_sum, high_arr_count)),
        format_optional(top[0][0]),
        format_optional(top[0][1]),
        format_optional(top[1][0]),
        format_optional(top[1][1]),
        format_optional(top[2][0]),
        format_optional(top[2][1]),
    ]

    buffer = StringIO()
    csv.writer(buffer, lineterminator="").writerow(fields)
    return buffer.getvalue()


def main() -> None:
    args = parse_args()
    conf = SparkConf().setAppName("analysis-3.2-spark-core")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")

    try:
        records = (
            sc.textFile(args.input if "://" in args.input else Path(args.input).resolve().as_uri())
            .mapPartitionsWithIndex(parse_partition)
            .persist(StorageLevel.MEMORY_AND_DISK)
        )

        band_stats = records.map(record_to_band).reduceByKey(merge_band)

        cause_counts = records.flatMap(record_to_causes).reduceByKey(
            lambda a, b: a + b
        )

        top_causes = (
            cause_counts.map(
                lambda item: (
                    (item[0][0], item[0][1]),
                    (item[0][2], item[1]),
                )
            )
            .combineByKey(
                create_top3,
                merge_top3_value,
                merge_top3_combiner,
            )
        )

        result = (
            band_stats.leftOuterJoin(top_causes)
            .map(format_result)
        )

        result.saveAsTextFile(args.output if "://" in args.output else Path(args.output).resolve().as_uri())

        print(f"[OK] Spark Core Job 2 written to: {Path(args.output).resolve()}")
    finally:
        try:
            records.unpersist()
        except UnboundLocalError:
            pass
        sc.stop()


if __name__ == "__main__":
    main()
