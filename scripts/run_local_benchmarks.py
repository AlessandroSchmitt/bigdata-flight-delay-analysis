#!/usr/bin/env python3
from __future__ import annotations

import argparse, csv, datetime as dt, json, os, shutil, subprocess, time
from pathlib import Path

TECHS = ("spark_sql", "spark_core", "hive")
SIZES = ("10pct", "50pct", "100pct")
JOBS = (1, 2)

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--runs", type=int, default=3)
    p.add_argument("--sizes", nargs="+", choices=SIZES, default=list(SIZES))
    p.add_argument("--jobs", nargs="+", type=int, choices=JOBS, default=list(JOBS))
    p.add_argument("--technologies", nargs="+", choices=TECHS, default=list(TECHS))
    p.add_argument("--results", default="benchmark-results/local_runs.csv")
    p.add_argument("--hive-container", default="hive4")
    p.add_argument("--no-resume", action="store_true")
    return p.parse_args()

def capture(cmd):
    r = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    return r.returncode, r.stdout.strip()

def ensure_tools(need_hive, hive_container):
    if shutil.which("spark-submit") is None:
        raise SystemExit("[ERROR] spark-submit not found in PATH")
    if not need_hive:
        return
    if shutil.which("docker") is None:
        raise SystemExit("[ERROR] docker not found in PATH")
    ok = subprocess.run(
        ["docker", "inspect", hive_container],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    ).returncode == 0
    if not ok:
        raise SystemExit(f"[ERROR] Hive container {hive_container!r} not found")
    code, running = capture(["docker", "inspect", "-f", "{{.State.Running}}", hive_container])
    if code != 0:
        raise SystemExit("[ERROR] cannot inspect Hive container")
    if running.lower() != "true":
        print(f"[INFO] starting {hive_container}")
        subprocess.run(["docker", "start", hive_container], check=True)
    deadline = time.time() + 90
    while time.time() < deadline:
        probe = subprocess.run(
            ["docker", "exec", hive_container, "beeline",
             "-u", "jdbc:hive2://localhost:10000/default",
             "-e", "SELECT 1;"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        if probe.returncode == 0:
            print(f"[OK] HiveServer2 ready: {hive_container}")
            return
        time.sleep(3)
    raise SystemExit("[ERROR] HiveServer2 did not become ready")

def git_commit(repo):
    code, out = capture(["git", "rev-parse", "HEAD"])
    return out if code == 0 else "unknown"

def load_manifest(repo):
    path = repo / "data/benchmark/manifest.json"
    if not path.is_file():
        raise SystemExit(
            "[ERROR] data/benchmark/manifest.json missing; "
            "run prepare_benchmark_inputs.py first"
        )
    return {x["label"]: x for x in json.loads(path.read_text())["inputs"]}

def load_completed(path):
    done = set()
    if not path.is_file():
        return done
    with path.open(newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if r["status"] == "ok":
                done.add((r["technology"], int(r["job"]), r["size_label"], int(r["run_number"])))
    return done

FIELDS = [
    "timestamp_utc","git_commit","technology","job","size_label","input_rows","input_bytes",
    "run_number","order_position","wall_seconds","status","exit_code","log_path","output_path"
]

def append_result(path, row):
    path.parent.mkdir(parents=True, exist_ok=True)
    new = not path.exists()
    with path.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        if new:
            w.writeheader()
        w.writerow(row)

def rotate(items, n):
    if not items:
        return []
    n %= len(items)
    return items[n:] + items[:n]

def spark_cmd(repo, tech, job, inp, out):
    folder = "spark-sql" if tech == "spark_sql" else "spark-core"
    return [
        "spark-submit",
        "--master", "local[2]",
        "--driver-memory", "3g",
        "--conf", f"spark.local.dir={repo / 'tmp/spark-benchmark'}",
        str(repo / folder / f"job_{job}.py"),
        "--input", str(inp),
        "--output", str(out),
    ]

def hive_cmd(repo, container, job, size, out):
    rel = out.resolve().relative_to(repo.resolve()).as_posix()
    return [
        "docker", "exec", container,
        "beeline",
        "-u", "jdbc:hive2://localhost:10000/default",
        "--hiveconf", f"INPUT=file:///workspace/data/benchmark/{size}",
        "--hiveconf", f"OUTPUT=file:///workspace/{rel}",
        "-f", f"/workspace/hive/job_{job}.hql",
    ]

def main():
    a = parse_args()
    if a.runs < 1:
        raise SystemExit("[ERROR] --runs must be >= 1")

    repo = Path(__file__).resolve().parents[1]
    os.chdir(repo)
    ensure_tools("hive" in a.technologies, a.hive_container)

    manifest = load_manifest(repo)
    for size in a.sizes:
        p = Path(manifest[size]["path"])
        if not p.is_file():
            raise SystemExit(f"[ERROR] benchmark input missing: {p}")

    results = (repo / a.results).resolve()
    done = set() if a.no_resume else load_completed(results)
    commit = git_commit(repo)
    techs = list(a.technologies)

    print(f"[INFO] results: {results}")
    print(f"[INFO] resume: {not a.no_resume}; successful rows already present: {len(done)}")

    for job in a.jobs:
        for size in a.sizes:
            meta = manifest[size]
            inp = Path(meta["path"]).resolve()
            for run in range(1, a.runs + 1):
                ordered = rotate(techs, run - 1)
                for pos, tech in enumerate(ordered, 1):
                    key = (tech, job, size, run)
                    if key in done:
                        print(f"[SKIP] job={job} size={size} run={run} tech={tech}")
                        continue

                    out = repo / f"outputs/benchmarks/local/job{job}/{size}/run{run}/{tech}"
                    log = repo / f"logs/benchmarks/local/job{job}/{size}/run{run}/{tech}.log"
                    shutil.rmtree(out, ignore_errors=True)
                    log.parent.mkdir(parents=True, exist_ok=True)

                    cmd = spark_cmd(repo, tech, job, inp, out) if tech != "hive" else \
                          hive_cmd(repo, a.hive_container, job, size, out)

                    print(f"[RUN] job={job} size={size} run={run}/{a.runs} pos={pos} tech={tech}")
                    start = time.perf_counter()
                    with log.open("w", encoding="utf-8") as f:
                        f.write("$ " + " ".join(cmd) + "\n\n")
                        f.flush()
                        proc = subprocess.run(cmd, stdout=f, stderr=subprocess.STDOUT, cwd=repo)
                    elapsed = time.perf_counter() - start
                    status = "ok" if proc.returncode == 0 else "failed"

                    append_result(results, {
                        "timestamp_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
                        "git_commit": commit,
                        "technology": tech,
                        "job": job,
                        "size_label": size,
                        "input_rows": meta["rows"],
                        "input_bytes": meta["bytes"],
                        "run_number": run,
                        "order_position": pos,
                        "wall_seconds": f"{elapsed:.6f}",
                        "status": status,
                        "exit_code": proc.returncode,
                        "log_path": str(log.relative_to(repo)),
                        "output_path": str(out.relative_to(repo)),
                    })
                    print(f"[{status.upper()}] {elapsed:.3f}s")

                    if proc.returncode != 0:
                        raise SystemExit(
                            f"[ERROR] benchmark failed; inspect {log}. "
                            "Completed rows are already saved; rerun to resume."
                        )

    print("[OK] requested local benchmarks completed")

if __name__ == "__main__":
    main()
