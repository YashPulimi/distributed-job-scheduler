import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from scheduler.api import create_app
from scheduler.config import Settings
from scheduler.store import IdempotencyConflict
from scheduler.worker import run_claim


def spec(priority=5, max_attempts=3, **extra):
    return {"kind": "sum", "payload": {"numbers": [1, 2, 3]}, "priority": priority,
            "max_attempts": max_attempts, "scheduled_at": None, **extra}


def due_now(store, job_id):
    with store.connect() as conn:
        conn.execute("UPDATE jobs SET available_at=clock_timestamp(),enqueued_at=clock_timestamp() WHERE id=%s", (job_id,))


def expire(store, job_id):
    with store.connect() as conn:
        conn.execute("UPDATE jobs SET lease_expires_at=clock_timestamp()-INTERVAL '1 second' WHERE id=%s", (job_id,))


def test_priority_and_fifo_for_equal_priority(store):
    low, _ = store.submit(spec(priority=0))
    first, _ = store.submit(spec(priority=10))
    second, _ = store.submit(spec(priority=10))
    assert [store.claim("w")["id"] for _ in range(3)] == [first["id"], second["id"], low["id"]]


def test_aging_promotes_old_low_priority_work(store):
    old, _ = store.submit(spec(priority=0))
    with store.connect() as conn:
        conn.execute("UPDATE jobs SET enqueued_at=clock_timestamp()-INTERVAL '1 hour' WHERE id=%s", (old["id"],))
    store.submit(spec(priority=10))
    assert store.claim("w")["id"] == old["id"]


def test_scheduled_job_is_not_claimed_early(store):
    store.submit(spec(scheduled_at=(datetime.now(timezone.utc)+timedelta(hours=1)).isoformat()))
    assert store.claim("w") is None


def test_concurrent_idempotent_submissions(store):
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda _: store.submit(spec(), "one-request"), range(16)))
    assert len({j["id"] for j, _ in results}) == 1
    assert sum(created for _, created in results) == 1
    with pytest.raises(IdempotencyConflict):
        store.submit(spec(priority=10), "one-request")


def test_concurrent_workers_claim_each_job_once(store):
    expected = {store.submit(spec())[0]["id"] for _ in range(40)}
    barrier = threading.Barrier(8)
    def drain(i):
        barrier.wait(timeout=10)
        claimed = []
        while (j := store.claim(f"worker-{i}", lease_seconds=120)):
            claimed.append(j["id"])
        return claimed
    with ThreadPoolExecutor(max_workers=8) as pool:
        actual = [j for group in pool.map(drain, range(8)) for j in group]
    assert len(actual) == len(set(actual)) == 40
    assert set(actual) == expected


def test_claim_skips_a_locked_job(store):
    first, _ = store.submit(spec(priority=10))
    second, _ = store.submit(spec(priority=0))
    with store.connect() as conn:
        conn.execute("SELECT id FROM jobs WHERE id=%s FOR UPDATE", (first["id"],))
        assert store.claim("w")["id"] == second["id"]


def test_retry_delay_and_final_failure(store):
    job, _ = store.submit(spec(max_attempts=2))
    first = store.claim("w")
    assert store.fail(first, "temporary failure", base_delay=10)
    assert store.claim("w") is None
    due_now(store, job["id"])
    second = store.claim("w")
    assert second["attempt_count"] == 2
    assert store.fail(second, "still failing")
    assert store.get(job["id"])["state"] == "failed"
    assert [r["outcome"] for r in store.history(job["id"])] == ["retry", "failed"]


def test_recovery_rejects_stale_results_and_heartbeats(store):
    row, _ = store.submit(spec())
    stale = store.claim("old-worker")
    expire(store, row["id"])
    assert not store.succeed(stale, {"bad": "expired"})
    assert not store.heartbeat(stale)
    assert store.recover_expired() == 1
    due_now(store, row["id"])
    current = store.claim("new-worker")
    assert current["claim_token"] != stale["claim_token"]
    assert not store.succeed(stale, {"bad": "stale"})
    assert not store.fail(stale, "stale failure")
    assert store.succeed(current, {"sum": 6})
    assert store.get(row["id"])["result"] == {"sum": 6}
    assert [r["outcome"] for r in store.history(row["id"])] == ["lease_expired", "succeeded"]


def test_crashes_respect_max_attempts(store):
    row, _ = store.submit(spec(max_attempts=1))
    store.claim("crashed")
    expire(store, row["id"])
    assert store.recover_expired() == 1
    assert store.get(row["id"])["state"] == "failed"
    assert store.claim("replacement") is None


def test_cancellation_prevents_claims(store):
    row, _ = store.submit(spec())
    assert store.cancel(row["id"])["state"] == "cancelled"
    assert store.claim("w") is None
    assert store.cancel(row["id"]) is None


def test_running_job_cannot_be_cancelled(store):
    row, _ = store.submit(spec())
    store.claim("w")
    assert store.cancel(row["id"]) is None


def test_heartbeat_preserves_long_running_job(store):
    row, _ = store.submit(spec(kind="demo", payload={"duration_seconds": 3, "fail_first_attempts": 0}))
    claimed = store.claim("worker", lease_seconds=2)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(run_claim, store, claimed, 2)
        time.sleep(2.2)
        assert store.recover_expired() == 0
        assert future.result(timeout=5)
    assert store.get(row["id"])["state"] == "succeeded"


def test_worker_runs_handler_and_persists_result(store):
    row, _ = store.submit(spec())
    assert run_claim(store, store.claim("w"))
    assert store.get(row["id"])["result"] == {"sum": 6, "count": 3}
    assert store.stats()["jobs"]["succeeded"] == 1


def test_api_end_to_end_and_idempotency_conflict(store):
    with TestClient(create_app(Settings(api_key="test"), store)) as c:
        headers = {"X-API-Key": "test", "Idempotency-Key": "api-test"}
        response = c.post("/jobs", json=spec(), headers=headers)
        assert response.status_code == 201
        job_id = response.json()["id"]
        assert "claim_token" not in response.json()
        assert c.post("/jobs", json=spec(), headers=headers).status_code == 200
        assert c.post("/jobs", json=spec(priority=10), headers=headers).status_code == 409
        assert run_claim(store, store.claim("test-worker"))
        result = c.get(f"/jobs/{job_id}", headers=headers).json()
        assert result["state"] == "succeeded" and result["result"]["sum"] == 6
        assert c.get(f"/jobs/{job_id}/attempts", headers=headers).json()[0]["outcome"] == "succeeded"
        assert c.post(f"/jobs/{job_id}/cancel", headers=headers).status_code == 409
