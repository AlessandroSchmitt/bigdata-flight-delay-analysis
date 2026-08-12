#!/usr/bin/env python3
"""
Prepare deterministic, distribution-preserving benchmark inputs.

10% and 50% are selected with a stable BLAKE2b hash threshold over each
canonical CSV record. This avoids using chronological prefixes (which could
change the month/airport distribution). The 10% sample is nested inside 50%.
The 100% input is hard-linked to the canonical file when possible.

Input preparation is outside benchmark timings.
"""
from __future__ import annotations

import argparse, hashlib, json, os, shutil
from pathlib import Path

MAX64 = 1 << 64
T10 = int(MAX64 * 0.10)
T50 = int(MAX64 * 0.50)

def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--input", default="data/processed/flights_cleaned.csv")
    p.add_argument("--output-root", default="data/benchmark")
    p.add_argument("--force", action="store_true")
    a = p.parse_args()

    src = Path(a.input).resolve()
    out = Path(a.output_root).resolve()
    if not src.is_file():
        raise SystemExit(f"[ERROR] missing canonical input: {src}")

    p10 = out / "10pct/flights_cleaned.csv"
    p50 = out / "50pct/flights_cleaned.csv"
    p100 = out / "100pct/flights_cleaned.csv"

    existing = [p10.exists(), p50.exists(), p100.exists()]
    if any(existing) and not a.force:
        raise SystemExit(
            "[ERROR] benchmark inputs already exist. "
            "Use --force to recreate all of them consistently."
        )

    for pth in (p10, p50, p100):
        pth.parent.mkdir(parents=True, exist_ok=True)
        if pth.exists():
            pth.unlink()

    total = n10 = n50 = 0
    with src.open("rb") as fin, p10.open("wb") as f10, p50.open("wb") as f50:
        header = fin.readline()
        if not header:
            raise SystemExit("[ERROR] canonical CSV is empty")
        f10.write(header)
        f50.write(header)

        for line in fin:
            total += 1
            h = int.from_bytes(hashlib.blake2b(line, digest_size=8).digest(), "big")
            if h < T50:
                f50.write(line)
                n50 += 1
                if h < T10:
                    f10.write(line)
                    n10 += 1

    try:
        os.link(src, p100)
        full_method = "hardlink"
    except OSError:
        shutil.copy2(src, p100)
        full_method = "copy"

    entries = [
        {
            "label": "10pct",
            "target_fraction": 0.10,
            "actual_fraction": n10 / total,
            "rows": n10,
            "bytes": p10.stat().st_size,
            "path": str(p10),
            "method": "blake2b_threshold",
        },
        {
            "label": "50pct",
            "target_fraction": 0.50,
            "actual_fraction": n50 / total,
            "rows": n50,
            "bytes": p50.stat().st_size,
            "path": str(p50),
            "method": "blake2b_threshold",
        },
        {
            "label": "100pct",
            "target_fraction": 1.00,
            "actual_fraction": 1.00,
            "rows": total,
            "bytes": p100.stat().st_size,
            "path": str(p100),
            "method": full_method,
        },
    ]

    manifest = {
        "canonical_input": str(src),
        "sampling": (
            "Stable BLAKE2b hash threshold per raw canonical CSV record; "
            "10pct is nested in 50pct; 100pct is the complete canonical input."
        ),
        "total_rows": total,
        "inputs": entries,
    }
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")

    print(f"[INFO] canonical rows: {total:,}")
    for e in entries:
        print(
            f"[OK] {e['label']}: rows={e['rows']:,} "
            f"fraction={e['actual_fraction']:.4f} "
            f"bytes={e['bytes']:,} method={e['method']}"
        )
    print(f"[OK] manifest: {out / 'manifest.json'}")

if __name__ == "__main__":
    main()
