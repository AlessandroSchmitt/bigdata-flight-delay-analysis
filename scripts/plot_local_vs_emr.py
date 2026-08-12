#!/usr/bin/env python3
"""Generate report-ready comparison charts for local vs EMR benchmark medians."""

from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from pathlib import Path

import matplotlib.pyplot as plt


TECH_LABELS = {
    "spark_sql": "Spark SQL",
    "spark_core": "Spark Core",
    "hive": "Hive / Tez",
}

TECH_ORDER = ("spark_sql", "spark_core", "hive")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--local", default="benchmark-results/local_summary.csv")
    p.add_argument("--emr", default="benchmark-results/emr_summary.csv")
    p.add_argument("--output-dir", default="benchmark-results/figures")
    return p.parse_args()


def read_summary(path: Path, environment: str) -> list[dict]:
    rows = []
    with path.open(newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            rows.append(
                {
                    "environment": environment,
                    "job": int(row["job"]),
                    "size_label": row["size_label"],
                    "technology": row["technology"],
                    "input_rows": int(row["input_rows"]),
                    "median_seconds": float(row["median_seconds"]),
                    "stdev_seconds": float(row["stdev_seconds"]),
                }
            )
    return rows


def plot_job(rows: list[dict], job: int, output_dir: Path) -> None:
    job_rows = [r for r in rows if r["job"] == job]
    if not job_rows:
        raise ValueError(f"No data for Job {job}")

    fig, ax = plt.subplots(figsize=(8.5, 5.4))

    for environment in ("local", "emr"):
        for tech in TECH_ORDER:
            values = sorted(
                [
                    r for r in job_rows
                    if r["environment"] == environment and r["technology"] == tech
                ],
                key=lambda r: r["input_rows"],
            )
            if not values:
                continue

            x = [r["input_rows"] / 1_000_000 for r in values]
            y = [r["median_seconds"] for r in values]
            err = [r["stdev_seconds"] for r in values]

            linestyle = "-" if environment == "local" else "--"
            label = f"{TECH_LABELS[tech]} — {'Local' if environment == 'local' else 'EMR'}"

            ax.errorbar(
                x,
                y,
                yerr=err,
                marker="o",
                linestyle=linestyle,
                capsize=4,
                label=label,
            )

    ax.set_xlabel("Input size (million records)")
    ax.set_ylabel("Median execution time (s)")
    ax.set_title(f"Local vs EMR execution time — Job {job}")
    ax.grid(True, alpha=0.25)
    ax.legend(ncol=2)
    fig.tight_layout()

    png = output_dir / f"job{job}_local_vs_emr.png"
    pdf = output_dir / f"job{job}_local_vs_emr.pdf"
    fig.savefig(png, dpi=200, bbox_inches="tight")
    fig.savefig(pdf, bbox_inches="tight")
    plt.close(fig)

    print(f"[OK] {png}")
    print(f"[OK] {pdf}")


def main() -> None:
    args = parse_args()
    local_path = Path(args.local)
    emr_path = Path(args.emr)
    output_dir = Path(args.output_dir)

    for path in (local_path, emr_path):
        if not path.is_file():
            raise SystemExit(f"[ERROR] Missing input file: {path}")

    output_dir.mkdir(parents=True, exist_ok=True)

    rows = read_summary(local_path, "local")
    rows += read_summary(emr_path, "emr")

    plot_job(rows, 1, output_dir)
    plot_job(rows, 2, output_dir)


if __name__ == "__main__":
    main()
