#!/usr/bin/env python3
"""Run reproducible EMR benchmarks for Spark SQL, Spark Core, and Hive/Tez."""

from __future__ import annotations

import argparse
import csv
import json
import statistics
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


TECH_ORDER = ("spark_sql", "spark_core", "hive")
SIZE_ROWS = {
    "10pct": 703895,
    "50pct": 3528413,
    "100pct": 7061582,
}
SIZE_BYTES = {
    "10pct": 30116080,
    "50pct": 150956451,
    "100pct": 302128941,
}

RESULT_FIELDS = [
    "timestamp_utc",
    "git_commit",
    "cluster_id",
    "technology",
    "job",
    "size_label",
    "input_rows",
    "input_bytes",
    "run_number",
    "order_position",
    "step_id",
    "creation_time_utc",
    "start_time_utc",
    "end_time_utc",
    "queue_seconds",
    "step_seconds",
    "client_seconds",
    "status",
    "failure_message",
    "output_path",
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--cluster-id", required=True)
    p.add_argument("--bucket", required=True)
    p.add_argument("--runs", type=int, default=3)
    p.add_argument("--sizes", nargs="+", default=["10pct", "50pct", "100pct"],
                   choices=list(SIZE_ROWS))
    p.add_argument("--jobs", nargs="+", type=int, default=[1, 2], choices=[1, 2])
    p.add_argument("--poll-seconds", type=float, default=10.0)
    p.add_argument("--results", default="benchmark-results/emr_runs.csv")
    p.add_argument("--environment", default="benchmark-results/emr_environment.json")
    p.add_argument("--no-resume", action="store_true")
    return p.parse_args()


def run(cmd: list[str], *, capture: bool = True, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(
        cmd,
        text=True,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.PIPE if capture else None,
        check=check,
    )


def aws_json(args: list[str]) -> dict | list:
    cp = run(["aws", *args, "--output", "json"])
    return json.loads(cp.stdout)


def git_commit() -> str:
    cp = run(["git", "rev-parse", "HEAD"])
    return cp.stdout.strip()


def ensure_clean_git() -> None:
    cp = run(["git", "status", "--porcelain"])
    if cp.stdout.strip():
        raise SystemExit(
            "[ERROR] Working tree is not clean. Commit/stash changes before official benchmarks."
        )


def cluster_state(cluster_id: str) -> str:
    cp = run([
        "aws", "emr", "describe-cluster",
        "--cluster-id", cluster_id,
        "--query", "Cluster.Status.State",
        "--output", "text",
    ])
    return cp.stdout.strip()


def write_environment(cluster_id: str, path: Path, commit: str) -> None:
    cluster = aws_json(["emr", "describe-cluster", "--cluster-id", cluster_id])["Cluster"]
    groups = aws_json(["emr", "list-instance-groups", "--cluster-id", cluster_id])["InstanceGroups"]
    region_cp = run(["aws", "configure", "get", "region"], check=False)
    region = region_cp.stdout.strip() or "unknown"

    payload = {
        "captured_at_utc": datetime.now(timezone.utc).isoformat(),
        "git_commit": commit,
        "cluster_id": cluster_id,
        "region": region,
        "release_label": cluster.get("ReleaseLabel"),
        "applications": [
            {"name": app.get("Name"), "version": app.get("Version")}
            for app in cluster.get("Applications", [])
        ],
        "subnet_id": cluster.get("Ec2InstanceAttributes", {}).get("Ec2SubnetId"),
        "service_role": cluster.get("ServiceRole"),
        "ec2_instance_profile": cluster.get("Ec2InstanceAttributes", {}).get("IamInstanceProfile"),
        "auto_termination_policy": cluster.get("AutoTerminationPolicy"),
        "instance_groups": [
            {
                "name": g.get("Name"),
                "type": g.get("InstanceGroupType"),
                "instance_type": g.get("InstanceType"),
                "market": g.get("Market"),
                "requested": g.get("RequestedInstanceCount"),
                "running": g.get("RunningInstanceCount"),
            }
            for g in groups
        ],
        "timing_definition": {
            "primary_metric": "step_seconds",
            "step_seconds": "EMR Step StartDateTime to EndDateTime",
            "queue_seconds": "CreationDateTime to StartDateTime",
            "client_seconds": "client wall clock from add-steps submission start until terminal step state observed",
        },
    }

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(f"[OK] environment: {path}")


def load_successful(path: Path) -> set[tuple[str, int, str, int]]:
    if not path.exists():
        return set()
    successes = set()
    with path.open(newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            if row.get("status") == "COMPLETED":
                successes.add((
                    row["technology"],
                    int(row["job"]),
                    row["size_label"],
                    int(row["run_number"]),
                ))
    return successes


def append_result(path: Path, row: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists()
    with path.open("a", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=RESULT_FIELDS)
        if not exists:
            writer.writeheader()
        writer.writerow({k: row.get(k, "") for k in RESULT_FIELDS})
        fh.flush()


def clean_output(s3_output: str) -> None:
    run(
        ["aws", "s3", "rm", s3_output, "--recursive", "--only-show-errors"],
        check=False,
    )


def step_spec(bucket: str, tech: str, job: int, size: str, output: str) -> str:
    if tech == "spark_sql":
        args = [
            "spark-submit", "--master", "yarn", "--deploy-mode", "cluster",
            f"s3://{bucket}/code/spark-sql/job_{job}.py",
            "--input", f"s3://{bucket}/benchmark/{size}/flights_cleaned.csv",
            "--output", output,
        ]
    elif tech == "spark_core":
        args = [
            "spark-submit", "--master", "yarn", "--deploy-mode", "cluster",
            f"s3://{bucket}/code/spark-core/job_{job}.py",
            "--input", f"s3://{bucket}/benchmark/{size}/flights_cleaned.csv",
            "--output", output,
        ]
    elif tech == "hive":
        args = [
            "hive",
            "--hiveconf", f"INPUT=s3://{bucket}/benchmark/{size}/",
            "--hiveconf", f"OUTPUT={output}",
            "-f", f"s3://{bucket}/code/hive/job_{job}.hql",
        ]
    else:
        raise ValueError(tech)

    name = f"bench-job{job}-{size}-{tech}"
    return (
        f"Type=CUSTOM_JAR,Name={name},ActionOnFailure=CONTINUE,"
        f"Jar=command-runner.jar,Args=[{','.join(args)}]"
    )


def add_step(cluster_id: str, spec: str) -> str:
    cp = run([
        "aws", "emr", "add-steps",
        "--cluster-id", cluster_id,
        "--steps", spec,
        "--query", "StepIds[0]",
        "--output", "text",
    ])
    return cp.stdout.strip()


def describe_step(cluster_id: str, step_id: str) -> dict:
    return aws_json([
        "emr", "describe-step",
        "--cluster-id", cluster_id,
        "--step-id", step_id,
    ])["Step"]["Status"]


def parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def seconds_between(a: str | None, b: str | None) -> float | None:
    da, db = parse_dt(a), parse_dt(b)
    if da is None or db is None:
        return None
    return (db - da).total_seconds()


def poll_step(cluster_id: str, step_id: str, poll_seconds: float) -> dict:
    last_state = None
    while True:
        status = describe_step(cluster_id, step_id)
        state = status["State"]
        if state != last_state:
            print(f"    state={state}")
            last_state = state

        if state in {"COMPLETED", "FAILED", "CANCELLED", "INTERRUPTED"}:
            return status

        time.sleep(poll_seconds)


def rotated_order(run_number: int) -> tuple[str, ...]:
    offset = (run_number - 1) % len(TECH_ORDER)
    return TECH_ORDER[offset:] + TECH_ORDER[:offset]


def main() -> None:
    args = parse_args()
    results_path = Path(args.results)
    environment_path = Path(args.environment)

    if args.runs < 1:
        raise SystemExit("[ERROR] --runs must be >= 1")

    ensure_clean_git()
    commit = git_commit()

    state = cluster_state(args.cluster_id)
    if state not in {"WAITING", "RUNNING"}:
        raise SystemExit(f"[ERROR] Cluster {args.cluster_id} is {state}, not WAITING/RUNNING.")

    write_environment(args.cluster_id, environment_path, commit)

    successful = set() if args.no_resume else load_successful(results_path)
    print(f"[INFO] results: {results_path}")
    print(f"[INFO] resume: {not args.no_resume}; successful keys already present: {len(successful)}")

    total_requested = len(args.jobs) * len(args.sizes) * args.runs * len(TECH_ORDER)
    completed_this_invocation = 0

    for job in args.jobs:
        for size in args.sizes:
            for run_number in range(1, args.runs + 1):
                order = rotated_order(run_number)
                for order_position, tech in enumerate(order, start=1):
                    key = (tech, job, size, run_number)
                    if key in successful:
                        print(f"[SKIP] job={job} size={size} run={run_number} tech={tech}")
                        continue

                    output = (
                        f"s3://{args.bucket}/outputs/benchmarks/emr/"
                        f"job{job}/{size}/run{run_number}/{tech}/"
                    )
                    clean_output(output)
                    spec = step_spec(args.bucket, tech, job, size, output)

                    print(
                        f"[RUN] job={job} size={size} run={run_number}/{args.runs} "
                        f"pos={order_position} tech={tech}"
                    )
                    client_start = time.perf_counter()
                    step_id = ""
                    status = None
                    failure_message = ""
                    try:
                        step_id = add_step(args.cluster_id, spec)
                        print(f"    step_id={step_id}")
                        status = poll_step(args.cluster_id, step_id, args.poll_seconds)
                    except Exception as exc:
                        client_seconds = time.perf_counter() - client_start
                        row = {
                            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                            "git_commit": commit,
                            "cluster_id": args.cluster_id,
                            "technology": tech,
                            "job": job,
                            "size_label": size,
                            "input_rows": SIZE_ROWS[size],
                            "input_bytes": SIZE_BYTES[size],
                            "run_number": run_number,
                            "order_position": order_position,
                            "step_id": step_id,
                            "client_seconds": f"{client_seconds:.6f}",
                            "status": "CLIENT_ERROR",
                            "failure_message": str(exc),
                            "output_path": output,
                        }
                        append_result(results_path, row)
                        raise

                    client_seconds = time.perf_counter() - client_start
                    timeline = status.get("Timeline", {})
                    creation = timeline.get("CreationDateTime")
                    start = timeline.get("StartDateTime")
                    end = timeline.get("EndDateTime")
                    failure = status.get("FailureDetails") or {}
                    failure_message = failure.get("Message", "")

                    row = {
                        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                        "git_commit": commit,
                        "cluster_id": args.cluster_id,
                        "technology": tech,
                        "job": job,
                        "size_label": size,
                        "input_rows": SIZE_ROWS[size],
                        "input_bytes": SIZE_BYTES[size],
                        "run_number": run_number,
                        "order_position": order_position,
                        "step_id": step_id,
                        "creation_time_utc": creation or "",
                        "start_time_utc": start or "",
                        "end_time_utc": end or "",
                        "queue_seconds": (
                            f"{seconds_between(creation, start):.6f}"
                            if seconds_between(creation, start) is not None else ""
                        ),
                        "step_seconds": (
                            f"{seconds_between(start, end):.6f}"
                            if seconds_between(start, end) is not None else ""
                        ),
                        "client_seconds": f"{client_seconds:.6f}",
                        "status": status["State"],
                        "failure_message": failure_message,
                        "output_path": output,
                    }
                    append_result(results_path, row)

                    if status["State"] != "COMPLETED":
                        raise SystemExit(
                            f"[ERROR] Step {step_id} ended as {status['State']}: {failure_message}"
                        )

                    successful.add(key)
                    completed_this_invocation += 1
                    print(
                        f"[OK] step_seconds={row['step_seconds']} "
                        f"queue_seconds={row['queue_seconds']}"
                    )

    print(
        f"[OK] requested EMR benchmarks completed "
        f"({completed_this_invocation} new successful measurements; "
        f"{total_requested} total requested keys)"
    )


if __name__ == "__main__":
    main()
