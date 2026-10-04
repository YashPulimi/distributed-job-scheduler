"""Measure one named run. No performance numbers are pre-filled."""
import argparse
import json
import math
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from uuid import uuid4

from demo import api


def percentile(values, p):
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * p) - 1)] if ordered else None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--jobs", type=int, default=100)
    parser.add_argument("--submitters", type=int, default=8)
    parser.add_argument("--duration", type=float, default=0.05)
    parser.add_argument("--timeout", type=float, default=300)
    parser.add_argument("--label", default="local-run")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if not 1 <= args.jobs <= 2000 or not 1 <= args.submitters <= 32 or not 0 <= args.duration <= 30:
        parser.error("jobs: 1..2000; submitters: 1..32; duration: 0..30 seconds")
    run = uuid4().hex
    started = time.perf_counter()

    def submit(i):
        status, job = api("POST", "/jobs", {
            "kind": "demo", "payload": {"duration_seconds": args.duration}, "priority": 5,
        }, f"benchmark-{run}-{i}")
        if status != 201:
            raise RuntimeError("Unexpected duplicate in fresh benchmark run")
        return job["id"]

    with ThreadPoolExecutor(max_workers=args.submitters) as pool:
        pending = set(pool.map(submit, range(args.jobs)))
    submission_seconds = time.perf_counter() - started
    outcomes = []
    deadline = time.monotonic() + args.timeout
    # Poll only this run's IDs, not the global /stats endpoint.
    while pending and time.monotonic() < deadline:
        with ThreadPoolExecutor(max_workers=args.submitters) as pool:
            rows = list(pool.map(lambda jid: api("GET", f"/jobs/{jid}")[1], list(pending)))
        for row in rows:
            if row["state"] in {"succeeded", "failed", "cancelled"}:
                pending.remove(row["id"])
                outcomes.append(row)
        if pending:
            time.sleep(0.25)
    elapsed = time.perf_counter() - started
    successful = [j for j in outcomes if j["state"] == "succeeded"]
    latency = [(datetime.fromisoformat(j["finished_at"])-datetime.fromisoformat(j["created_at"])).total_seconds()
               for j in successful]
    result = {
        "run_id": run, "label": args.label, "submitted": args.jobs,
        "submission_seconds": round(submission_seconds, 3), "observed_wall_seconds": round(elapsed, 3),
        "succeeded": len(successful), "failed": sum(j["state"] == "failed" for j in outcomes),
        "unfinished": len(pending), "observed_successes_per_second": round(len(successful)/elapsed, 3),
        "p50_completion_seconds": percentile(latency, 0.5), "p95_completion_seconds": percentile(latency, 0.95),
        "total_attempts_finished_jobs": sum(j["attempt_count"] for j in outcomes),
        "handler_duration_seconds": args.duration,
        "notes": "Wall throughput includes submission and polling. Latency includes queue time and execution. Record hardware and worker settings separately.",
    }
    output = json.dumps(result, indent=2)
    print(output)
    if args.output:
        args.output.write_text(output + "\n")
    if pending or len(successful) != args.jobs:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

