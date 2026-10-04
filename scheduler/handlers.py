"""Allowlisted, bounded demo handlers. Never execute client-provided code."""
import re
from collections import Counter

from scheduler.models import PAYLOAD_TYPES


class LeaseLost(Exception):
    pass


def execute(job, lease_lost):
    payload = PAYLOAD_TYPES[job["kind"]].model_validate(job["payload"])
    if lease_lost.is_set():
        raise LeaseLost("lease lost before execution")
    if job["kind"] == "sum":
        return {"sum": sum(payload.numbers), "count": len(payload.numbers)}
    if job["kind"] == "word_count":
        words = re.findall(r"\b\w+\b", payload.text.lower())
        counts = Counter(words)
        return {"total_words": len(words), "unique_words": len(counts), "top_10": counts.most_common(10)}
    if job["kind"] == "demo":
        if lease_lost.wait(payload.duration_seconds):
            raise LeaseLost("lease lost while waiting")
        if job["attempt_count"] <= payload.fail_first_attempts:
            raise RuntimeError("intentional demo failure")
        return {"message": "completed", "attempt": job["attempt_count"]}
    raise ValueError("unsupported handler")

