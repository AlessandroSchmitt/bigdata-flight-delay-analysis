#!/usr/bin/env python3
"""Summarize successful EMR benchmark runs using step_seconds as primary metric."""

from __future__ import annotations

import argparse
import csv
import statistics
from collections import defaultdict
from pathlib import Path


TECH_ORDER = {"spark_sql": 0, "spark_core": 1, "hive": 2}
SIZE_ORDER = {"10pct": 0, "50pct": 1, "100pct": 2}

FIELDS = [
    "job",
    "size_label",
    "technology",
    "input_rows",
    "input_bytes",
    "n_runs",
    "min_seconds",
    "median_seconds",
    "mean_seconds",
    "max_seconds",
    "stdev_seconds",
    "median_queue_seconds",
]


def parse_args():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--input", default="benchmark-results/emr_runs.csv")
    p.add_argument("--output", default="benchmark-results/emr_summary.csv")
    return p.parse_args()


def main():
    args = parse_args()
    input_path = Path(args.input)
    output_path = Path(args.output)

    if not input_path.is_file():
        raise SystemExit(f"[ERROR] Missing input: {input_path}")

    groups = defaultdict(list)
    with input_path.open(newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            if row.get("status") != "COMPLETED":
                continue
            if not row.get("step_seconds"):
                continue
            key = (int(row["job"]), row["size_label"], row["technology"])
            groups[key].append(row)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=FIELDS)
        writer.writeheader()

        ordered_keys = sorted(
            groups,
            key=lambda k: (k[0], SIZE_ORDER.get(k[1], 99), TECH_ORDER.get(k[2], 99)),
        )

        print(f"{'JOB':<4} {'SIZE':<7} {'TECH':<12} {'N':>2} {'MEDIAN(s)':>10} {'MEAN(s)':>10}")
        print("-" * 54)

        for key in ordered_keys:
            rows = groups[key]
            times = [float(r["step_seconds"]) for r in rows]
            queues = [
                float(r["queue_seconds"])
                for r in rows
                if r.get("queue_seconds")
            ]
            first = rows[0]
            record = {
                "job": key[0],
                "size_label": key[1],
                "technology": key[2],
                "input_rows": first["input_rows"],
                "input_bytes": first["input_bytes"],
                "n_runs": len(times),
                "min_seconds": f"{min(times):.6f}",
                "median_seconds": f"{statistics.median(times):.6f}",
                "mean_seconds": f"{statistics.mean(times):.6f}",
                "max_seconds": f"{max(times):.6f}",
                "stdev_seconds": (
                    f"{statistics.stdev(times):.6f}" if len(times) > 1 else "0.000000"
                ),
                "median_queue_seconds": (
                    f"{statistics.median(queues):.6f}" if queues else ""
                ),
            }
            writer.writerow(record)

            print(
                f"{key[0]:<4} {key[1]:<7} {key[2]:<12} {len(times):>2} "
                f"{statistics.median(times):>10.3f} {statistics.mean(times):>10.3f}"
            )

    print(f"[OK] summary: {output_path}")


if __name__ == "__main__":
    main()
