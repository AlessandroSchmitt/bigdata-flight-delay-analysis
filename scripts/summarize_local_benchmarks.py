#!/usr/bin/env python3
from __future__ import annotations
import argparse, csv, statistics
from collections import defaultdict
from pathlib import Path

SIZE_ORDER = {"10pct": 0, "50pct": 1, "100pct": 2}
TECH_ORDER = {"spark_sql": 0, "spark_core": 1, "hive": 2}

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input", default="benchmark-results/local_runs.csv")
    p.add_argument("--output", default="benchmark-results/local_summary.csv")
    a = p.parse_args()

    inp = Path(a.input)
    out = Path(a.output)
    if not inp.is_file():
        raise SystemExit(f"[ERROR] missing results: {inp}")

    groups = defaultdict(list)
    meta = {}
    with inp.open(newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if r["status"] != "ok":
                continue
            key = (int(r["job"]), r["size_label"], r["technology"])
            groups[key].append(float(r["wall_seconds"]))
            meta[key] = (int(r["input_rows"]), int(r["input_bytes"]))

    rows = []
    for key, vals in groups.items():
        job, size, tech = key
        vals = sorted(vals)
        nrows, nbytes = meta[key]
        rows.append({
            "job": job,
            "size_label": size,
            "technology": tech,
            "input_rows": nrows,
            "input_bytes": nbytes,
            "n_runs": len(vals),
            "min_seconds": min(vals),
            "median_seconds": statistics.median(vals),
            "mean_seconds": statistics.mean(vals),
            "max_seconds": max(vals),
            "stdev_seconds": statistics.stdev(vals) if len(vals) > 1 else 0.0,
        })

    rows.sort(key=lambda r: (
        r["job"],
        SIZE_ORDER.get(r["size_label"], 99),
        TECH_ORDER.get(r["technology"], 99),
    ))

    fields = [
        "job","size_label","technology","input_rows","input_bytes","n_runs",
        "min_seconds","median_seconds","mean_seconds","max_seconds","stdev_seconds"
    ]
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            rr = dict(r)
            for k in ("min_seconds","median_seconds","mean_seconds","max_seconds","stdev_seconds"):
                rr[k] = f"{r[k]:.6f}"
            w.writerow(rr)

    print(f"[OK] summary: {out}")
    print(f"{'JOB':<4} {'SIZE':<7} {'TECH':<11} {'N':>3} {'MEDIAN(s)':>10} {'MEAN(s)':>10}")
    print("-" * 52)
    for r in rows:
        print(
            f"{r['job']:<4} {r['size_label']:<7} {r['technology']:<11} "
            f"{r['n_runs']:>3} {r['median_seconds']:>10.3f} {r['mean_seconds']:>10.3f}"
        )

if __name__ == "__main__":
    main()
