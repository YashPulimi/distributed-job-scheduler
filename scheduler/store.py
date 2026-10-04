"""Short transactions; no database locks are held while handlers execute."""

import hashlib
import json
import re
from importlib.resources import files
from uuid import uuid4

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb


class IdempotencyConflict(Exception):
    pass


def retry_delay(attempt: int, base: float = 1.0) -> float:
    return min(60.0, base * 2 ** min(max(attempt - 1, 0), 20))


class Store:
    def __init__(self, dsn: str, schema: str = "public", aging_seconds: int = 30):
        if not re.fullmatch(r"[a-z_][a-z0-9_]{0,62}", schema):
            raise ValueError("Invalid database schema name")
        if aging_seconds < 1:
            raise ValueError("aging_seconds must be positive")
        self.dsn, self.schema, self.aging_seconds = dsn, schema, aging_seconds

    def connect(self):
        return psycopg.connect(
            self.dsn,
            row_factory=dict_row,
            connect_timeout=5,
            options=f"-c search_path={self.schema} -c statement_timeout=5000 -c lock_timeout=3000",
        )

    def initialize(self):
        with self.connect() as conn:
            conn.execute(files("scheduler").joinpath("schema.sql").read_text())

    def ping(self):
        with self.connect() as conn:
            conn.execute("SELECT id FROM jobs LIMIT 1")

    def submit(self, request: dict, idempotency_key: str | None = None):
        canonical = json.dumps(request, sort_keys=True, separators=(",", ":"), allow_nan=False)
        fingerprint = hashlib.sha256(canonical.encode()).hexdigest()
        with self.connect() as conn:
            row = conn.execute(
                """INSERT INTO jobs (id, idempotency_key, request_hash, kind, payload,
                       priority, max_attempts, available_at, enqueued_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,
                       GREATEST(COALESCE(%s::timestamptz,clock_timestamp()),clock_timestamp()),
                       GREATEST(COALESCE(%s::timestamptz,clock_timestamp()),clock_timestamp()))
                   ON CONFLICT (idempotency_key) DO NOTHING RETURNING *""",
                (uuid4(), idempotency_key, fingerprint, request["kind"], Jsonb(request["payload"]),
                 request.get("priority", 5), request.get("max_attempts", 3),
                 request.get("scheduled_at"), request.get("scheduled_at")),
            ).fetchone()
            if row:
                return row, True
            row = conn.execute("SELECT * FROM jobs WHERE idempotency_key=%s", (idempotency_key,)).fetchone()
            if row["request_hash"] != fingerprint:
                raise IdempotencyConflict("Idempotency key already used for a different request")
            return row, False

    def get(self, job_id):
        with self.connect() as conn:
            return conn.execute("SELECT * FROM jobs WHERE id=%s", (job_id,)).fetchone()

    def list(self, state=None, limit=50):
        with self.connect() as conn:
            return conn.execute(
                "SELECT * FROM jobs WHERE (%s::text IS NULL OR state=%s) ORDER BY sequence DESC LIMIT %s",
                (state, state, limit),
            ).fetchall()

    def history(self, job_id):
        with self.connect() as conn:
            return conn.execute("SELECT * FROM attempts WHERE job_id=%s ORDER BY number", (job_id,)).fetchall()

    def claim(self, worker_id, lease_seconds=15):
        token = uuid4()
        with self.connect() as conn:
            # Aging promotes waiting jobs. Under contention, locked rows are skipped;
            # priority describes claim preference, not global start/completion order.
            job = conn.execute(
                """WITH candidate AS (
                     SELECT id FROM jobs WHERE state='queued' AND available_at<=clock_timestamp()
                       AND attempt_count<max_attempts
                     ORDER BY priority + FLOOR(EXTRACT(EPOCH FROM
                       (statement_timestamp()-enqueued_at)) / %s) DESC, sequence ASC
                     FOR UPDATE SKIP LOCKED LIMIT 1
                   ) UPDATE jobs j SET state='running', attempt_count=j.attempt_count+1,
                     claim_token=%s, worker_id=%s,
                     lease_expires_at=clock_timestamp()+(%s * INTERVAL '1 second'),
                     updated_at=clock_timestamp(), last_error=NULL
                   FROM candidate c WHERE j.id=c.id RETURNING j.*""",
                (self.aging_seconds, token, worker_id, lease_seconds),
            ).fetchone()
            if job:
                conn.execute(
                    "INSERT INTO attempts(job_id,number,claim_token,worker_id) VALUES (%s,%s,%s,%s)",
                    (job["id"], job["attempt_count"], token, worker_id),
                )
            return job

    def heartbeat(self, job, lease_seconds=15):
        with self.connect() as conn:
            return conn.execute(
                """UPDATE jobs SET lease_expires_at=clock_timestamp()+(%s * INTERVAL '1 second'),
                   updated_at=clock_timestamp() WHERE id=%s AND claim_token=%s AND state='running'
                   AND lease_expires_at>clock_timestamp() RETURNING id""",
                (lease_seconds, job["id"], job["claim_token"]),
            ).fetchone() is not None

    def succeed(self, job, result):
        with self.connect() as conn:
            row = conn.execute(
                """UPDATE jobs SET state='succeeded',result=%s,finished_at=clock_timestamp(),
                   updated_at=clock_timestamp(),claim_token=NULL,worker_id=NULL,lease_expires_at=NULL
                   WHERE id=%s AND claim_token=%s AND state='running'
                   AND lease_expires_at>clock_timestamp() RETURNING id""",
                (Jsonb(result), job["id"], job["claim_token"]),
            ).fetchone()
            if row:
                conn.execute(
                    "UPDATE attempts SET outcome='succeeded',finished_at=clock_timestamp() WHERE claim_token=%s",
                    (job["claim_token"],),
                )
            return row is not None

    def fail(self, job, error, base_delay=1.0):
        terminal = job["attempt_count"] >= job["max_attempts"]
        state, outcome = ("failed", "failed") if terminal else ("queued", "retry")
        delay = retry_delay(job["attempt_count"], base_delay)
        with self.connect() as conn:
            row = conn.execute(
                """UPDATE jobs SET state=%s,last_error=%s,updated_at=clock_timestamp(),
                   finished_at=CASE WHEN %s THEN clock_timestamp() ELSE NULL END,
                   available_at=clock_timestamp()+(%s * INTERVAL '1 second'),
                   enqueued_at=clock_timestamp()+(%s * INTERVAL '1 second'),
                   claim_token=NULL,worker_id=NULL,lease_expires_at=NULL
                   WHERE id=%s AND claim_token=%s AND state='running'
                   AND lease_expires_at>clock_timestamp() RETURNING id""",
                (state, str(error)[:1000], terminal, delay, delay, job["id"], job["claim_token"]),
            ).fetchone()
            if row:
                conn.execute(
                    "UPDATE attempts SET outcome=%s,error=%s,finished_at=clock_timestamp() WHERE claim_token=%s",
                    (outcome, str(error)[:1000], job["claim_token"]),
                )
            return row is not None

    def recover_expired(self, limit=100):
        with self.connect() as conn:
            rows = conn.execute(
                """SELECT * FROM jobs WHERE state='running' AND lease_expires_at<=clock_timestamp()
                   ORDER BY lease_expires_at FOR UPDATE SKIP LOCKED LIMIT %s""", (limit,),
            ).fetchall()
            for job in rows:
                terminal = job["attempt_count"] >= job["max_attempts"]
                delay = retry_delay(job["attempt_count"])
                conn.execute(
                    """UPDATE jobs SET state=%s,last_error='worker lease expired',
                       available_at=clock_timestamp()+(%s * INTERVAL '1 second'),
                       enqueued_at=clock_timestamp()+(%s * INTERVAL '1 second'),
                       updated_at=clock_timestamp(),finished_at=CASE WHEN %s THEN clock_timestamp() ELSE NULL END,
                       claim_token=NULL,worker_id=NULL,lease_expires_at=NULL WHERE id=%s""",
                    ("failed" if terminal else "queued", delay, delay, terminal, job["id"]),
                )
                conn.execute(
                    """UPDATE attempts SET outcome='lease_expired',error='worker lease expired',
                       finished_at=clock_timestamp() WHERE claim_token=%s""", (job["claim_token"],),
                )
            return len(rows)

    def cancel(self, job_id):
        with self.connect() as conn:
            return conn.execute(
                """UPDATE jobs SET state='cancelled',finished_at=clock_timestamp(),updated_at=clock_timestamp()
                   WHERE id=%s AND state='queued' RETURNING *""", (job_id,),
            ).fetchone()

    def stats(self):
        with self.connect() as conn:
            counts = {r["state"]: r["count"] for r in conn.execute(
                "SELECT state,count(*) FROM jobs GROUP BY state"
            ).fetchall()}
            timing = conn.execute(
                """SELECT COUNT(*) AS completed,
                   percentile_cont(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (finished_at-created_at))) AS p50_seconds,
                   percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (finished_at-created_at))) AS p95_seconds
                   FROM jobs WHERE state='succeeded'"""
            ).fetchone()
            return {"jobs": {s: counts.get(s, 0) for s in
                ["queued", "running", "succeeded", "failed", "cancelled"]}, "completion_latency": timing}

