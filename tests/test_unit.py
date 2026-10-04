import threading
from datetime import timezone

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from scheduler.api import create_app
from scheduler.config import Settings
from scheduler.handlers import LeaseLost, execute
from scheduler.models import DemoPayload, JobRequest, SumPayload
from scheduler.store import retry_delay, Store


def job(kind, payload, attempt=1):
    return {"kind": kind, "payload": payload, "attempt_count": attempt}


def test_sum_including_negative_values():
    assert execute(job("sum", {"numbers": [10, -2, 4]}), threading.Event()) == {"sum": 12, "count": 3}


def test_word_count_normalizes_punctuation_and_case():
    result = execute(job("word_count", {"text": "Hello, hello! World."}), threading.Event())
    assert result == {"total_words": 3, "unique_words": 2, "top_10": [("hello", 2), ("world", 1)]}


def test_demo_fails_then_succeeds():
    payload = {"duration_seconds": 0, "fail_first_attempts": 1}
    with pytest.raises(RuntimeError, match="intentional"):
        execute(job("demo", payload, 1), threading.Event())
    assert execute(job("demo", payload, 2), threading.Event())["attempt"] == 2


def test_lost_lease_stops_cooperative_work():
    lost = threading.Event()
    lost.set()
    with pytest.raises(LeaseLost):
        execute(job("demo", {"duration_seconds": 30}), lost)


@pytest.mark.parametrize("numbers", [[], [True], [1.5], [10**10], list(range(10001))])
def test_reject_invalid_sum_payload(numbers):
    with pytest.raises(ValidationError):
        SumPayload(numbers=numbers)


@pytest.mark.parametrize("duration", [-1, 31, float("inf"), float("nan")])
def test_demo_duration_is_bounded(duration):
    with pytest.raises(ValidationError):
        DemoPayload(duration_seconds=duration)


def test_retry_backoff_has_a_cap():
    assert [retry_delay(i) for i in (1, 2, 3, 6, 7, 10)] == [1, 2, 4, 32, 60, 60]


def test_schedule_requires_timezone():
    with pytest.raises(ValidationError):
        JobRequest(kind="sum", payload={"numbers": [1]}, scheduled_at="2030-01-01T00:00:00")
    req = JobRequest(kind="sum", payload={"numbers": [1]}, scheduled_at="2030-01-01T02:00:00+02:00")
    assert req.scheduled_at.hour == 0
    assert req.scheduled_at.tzinfo == timezone.utc


@pytest.mark.parametrize("priority", [-1, 11, "5", True])
def test_priority_is_a_bounded_integer(priority):
    with pytest.raises(ValidationError):
        JobRequest(kind="sum", payload={}, priority=priority)


def test_unknown_fields_and_handlers_are_rejected():
    with pytest.raises(ValidationError):
        JobRequest(kind="shell", payload={"command": "echo hello"})
    with pytest.raises(ValidationError):
        SumPayload(numbers=[1], extra=True)


def test_schema_identifier_is_validated():
    with pytest.raises(ValueError):
        Store("unused", "public; DROP TABLE jobs")


class PingOnlyStore:
    def ping(self):
        pass


def test_api_auth_and_validation_without_database():
    # Deliberately does NOT fake transactions or claim concurrency guarantees.
    with TestClient(create_app(Settings(api_key="test-key"), PingOnlyStore())) as client:
        assert client.get("/health").status_code == 200
        assert client.get("/jobs").status_code == 401
        assert client.get("/jobs", headers={"X-API-Key": "wrong"}).status_code == 401
        assert client.post("/jobs", headers={"X-API-Key": "test-key"}, json={
            "kind": "sum", "payload": {"numbers": [True]}
        }).status_code == 422


def test_invalid_worker_config():
    with pytest.raises(ValueError):
        Settings(lease_seconds=0.1)
    with pytest.raises(ValueError):
        Settings(concurrency=0)

