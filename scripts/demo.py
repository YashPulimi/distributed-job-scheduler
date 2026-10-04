"""Run inside the API container: python scripts/demo.py"""
import json
import os
import time
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from uuid import uuid4


BASE = os.getenv("API_URL", "http://127.0.0.1:8000").rstrip("/")
KEY = os.getenv("API_KEY", "local-development-only")


def api(method, path, body=None, key=None):
    headers = {"X-API-Key": KEY, "Content-Type": "application/json"}
    if key:
        headers["Idempotency-Key"] = key
    data = json.dumps(body).encode() if body is not None else None
    request = Request(BASE + path, data=data, headers=headers, method=method)
    try:
        with urlopen(request, timeout=10) as response:
            return response.status, json.load(response)
    except HTTPError as exc:
        raise RuntimeError(f"HTTP {exc.code}: {exc.read().decode()}") from None


def wait_for(job_id, timeout=90):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        _, job = api("GET", f"/jobs/{job_id}")
        if job["state"] in {"succeeded", "failed", "cancelled"}:
            return job
        time.sleep(0.25)
    raise TimeoutError(f"Job {job_id} did not finish in {timeout}s. Check worker logs.")


def main():
    prefix = uuid4().hex
    body = {"kind": "sum", "payload": {"numbers": [10, 20, 30]}, "priority": 8}
    _, first = api("POST", "/jobs", body, prefix + "-sum")
    status, replay = api("POST", "/jobs", body, prefix + "-sum")
    assert status == 200 and first["id"] == replay["id"], "Idempotent submission failed"
    finished = wait_for(first["id"])
    assert finished["state"] == "succeeded" and finished["result"]["sum"] == 60
    print("PASS: duplicate submission returned the same job; sum result is 60")

    _, retry = api("POST", "/jobs", {
        "kind": "demo", "payload": {"duration_seconds": 0.1, "fail_first_attempts": 2},
        "max_attempts": 3,
    }, prefix + "-retry")
    finished = wait_for(retry["id"])
    assert finished["state"] == "succeeded" and finished["attempt_count"] == 3
    _, attempts = api("GET", f"/jobs/{retry['id']}/attempts")
    assert [a["outcome"] for a in attempts] == ["retry", "retry", "succeeded"]
    print("PASS: two failed attempts followed by success; history is recorded")
    print(json.dumps({"sum_job": first["id"], "retry_job": retry["id"], "stats": api("GET", "/stats")[1]}, indent=2))


if __name__ == "__main__":
    main()

