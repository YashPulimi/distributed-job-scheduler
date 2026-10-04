# Build and understand the scheduler step by step

Work through these milestones in order. Save your code changes with Git after each milestone. Do not add performance numbers to your resume until you have measured them yourself.

## 1. Run the supplied project

Install and start Docker Desktop (Windows/macOS) or Docker Engine plus Compose v2 (Linux). Extract the ZIP. In a terminal, `cd` into `distributed-job-scheduler`, the directory containing `compose.yaml`.

```bash
docker --version
docker compose version
docker compose up --build -d
docker compose ps
docker compose logs api
```

Expected: PostgreSQL healthy, migration exited 0, API healthy, worker running. A first build needs internet access for the Python and PostgreSQL images and Python packages. If port 8000 is occupied, change only the host port in Compose to `127.0.0.1:8001:8000` and open port 8001 in your browser. The internal demo still uses port 8000.

Open <http://localhost:8000/docs>. Swagger's route forms expose the `x-api-key` header. Enter `local-development-only`, unless you changed `.env`. Job POST requests also support an optional `idempotency-key` header.

## 2. Submit work and inspect a result

The easiest cross-platform starting command is:

```bash
docker compose exec api python scripts/demo.py
```

It submits an addition job twice with the same idempotency key, verifies one job ID, checks the result `60`, and runs a task that deliberately fails twice before succeeding on attempt three. It prints the job IDs.

For a manual request, POST this JSON to `/jobs` in the API docs:

```json
{
  "kind": "word_count",
  "payload": {"text": "distributed systems need reliable workers reliable workers"},
  "priority": 7,
  "max_attempts": 3
}
```

Copy the returned job ID into GET `/jobs/{job_id}`. Inspect `state`, `attempt_count`, and `result`. Then inspect GET `/jobs/{job_id}/attempts`.

**Read next:** `models.py`, `api.py`, `handlers.py`. Explain how a request becomes a validated, persisted job. Add a unit test for empty input in the word-count handler.

## 3. Understand the database and claim transaction

Read `schema.sql`, then `Store.submit()` and `Store.claim()` in `store.py`.

Important distinctions:

1. A job is the requested work; an attempt is one execution of that work.
2. The transaction claims a row, assigns a fresh token, increments attempts, and records attempt history atomically.
3. `FOR UPDATE SKIP LOCKED` prevents workers from waiting on the same locked candidate.
4. The transaction commits before the handler runs. Holding a transaction open while a 30-second handler runs would consume locks and database resources unnecessarily.

Inspect the tables:

```bash
docker compose exec db psql -U scheduler -d scheduler -c "SELECT id,state,priority,attempt_count,worker_id FROM jobs ORDER BY sequence DESC LIMIT 10;"
docker compose exec db psql -U scheduler -d scheduler -c "SELECT job_id,number,outcome FROM attempts ORDER BY started_at DESC LIMIT 10;"
```

**Your exercise:** draw the state machine from the README without looking at it. Explain why a Python process-local lock would not protect multiple worker containers.

## 4. Run the automated tests

```bash
docker compose --profile test run --build --rm test
```

This starts the test process with an actual PostgreSQL connection. Every database test creates its own random schema and drops only that schema afterward. It does not truncate your application tables.

The initial suite contains **37 cases: 23 without a database and 14 PostgreSQL integration cases**. Your result should show all passing with zero skips. If a test fails, fix it before using the project as completed resume evidence. Do not convert a failure into a skip.

For faster edits to pure logic, with Python 3.12 installed locally:

```bash
python -m venv .venv
```

Activate with `.venv\Scripts\Activate.ps1` on Windows PowerShell or `source .venv/bin/activate` on macOS/Linux, then:

```bash
python -m pip install -r requirements.lock
python -m pip install --no-deps -e .
python -m pytest tests/test_unit.py -v
```

The full local suite needs `TEST_DATABASE_URL` pointing to a disposable PostgreSQL database whose user can create/drop schemas. Prefer Compose if you do not already run PostgreSQL.

## 5. Try priorities, scheduling, and aging

For an easy-to-observe experiment, stop workers while submitting jobs:

```bash
docker compose stop worker
```

Submit three jobs in the API docs, with priorities 1, 10, and 5. Use `demo` payload `{"duration_seconds":2}`. Set `WORKER_CONCURRENCY=1` in `.env` (copy `.env.example` first), then recreate the worker:

```bash
docker compose up -d --force-recreate --no-deps worker
docker compose logs -f worker
```

For recently submitted jobs, priority 10 should be claimed first, then 5, then 1. Existing older jobs can change that outcome because aging is intentional. The automated test isolates this experiment. Press Ctrl+C to exit log viewing; it does not stop the container.

Submit a future `scheduled_at` timestamp with a timezone, for example a timestamp five minutes ahead in UTC ending in `Z`. Confirm it remains queued until eligible. The queue uses database time.

**Your exercise:** compare strict priority to priority with aging. Explain why priority alone can delay low-priority work indefinitely when higher-priority jobs keep arriving.

## 6. Demonstrate worker-crash recovery

Use one worker container for this experiment. Keep `LEASE_SECONDS=15` and submit a `demo` job with `duration_seconds=20`, `fail_first_attempts=0`, and `max_attempts=3`. Poll its GET endpoint until it says `running`.

Immediately stop the worker abruptly:

```bash
docker compose kill -s SIGKILL worker
docker compose up -d --no-deps worker
```

Inspect the same job for roughly 40 seconds. The replacement worker waits until the old lease expires, records a `lease_expired` attempt, applies backoff, and runs a new attempt. Expect eventual success and two attempts if you killed it while its first attempt was active. If it finished before the kill, retry with a new job.

The integration test also verifies that the old claim token cannot save a result or extend its lease after expiry or reassignment. A delayed worker may still execute code, which is why external side effects need their own idempotency protection.

**Your exercise:** explain the difference between duplicate submission and duplicate execution. Explain the crash window after sending an email but before marking a job complete.

## 7. Measure performance honestly

Start with one container and one slot (`WORKER_CONCURRENCY=1` in `.env`), then run:

```bash
docker compose up -d --force-recreate --no-deps --scale worker=1 worker
docker compose exec api python scripts/benchmark.py --jobs 200 --duration 0.05 --label one-slot
```

Save the printed JSON as `benchmark-one-slot.json` on your computer. Wait until the queue is empty, set `WORKER_CONCURRENCY=4`, and run:

```bash
docker compose up -d --force-recreate --no-deps --scale worker=2 worker
docker compose exec api python scripts/benchmark.py --jobs 200 --duration 0.05 --label eight-slots
```

Now you have two independent worker containers and eight execution slots in total. Repeat each configuration three times. Record your CPU, RAM, Docker allocation, OS, Python/PostgreSQL versions, slot count, and payload duration. Use the same workload in each comparison.

The script reports successful jobs per observed wall second and p50/p95 end-to-end latency using only IDs from that benchmark run. Throughput includes request submission and polling overhead. The sample sleep workload is a synthetic I/O-like workload, not evidence of real customer traffic or CPU scaling. Do not expect linear speedup: database connection setup and polling add overhead.

**Your exercise:** inspect slow queries and propose a connection pool. Avoid changing several parameters at once; measure each change separately.

## 8. Add one feature yourself

Pick one after the baseline tests pass:

- **Execution timeout:** run handlers in subprocesses, terminate the process after a configured deadline, and test cleanup. Thread cancellation alone cannot safely stop arbitrary code.
- **Transactional outbox:** store intended notifications in the same transaction as a state change; implement a separately retried delivery worker with downstream idempotency.
- **Connection pooling:** add a bounded Psycopg pool, test exhaustion behavior, and compare throughput with the baseline.
- **Queue admission control:** cap queued jobs per authenticated tenant and reject excess work with a clear retry policy. This requires real tenant identity first.

For the first extension, write the failure test before implementing the change. This produces code you can explain as your own contribution instead of only running supplied code.

## 9. Put it on your GitHub

Use the GitHub account at <https://github.com/YashPulimi>. Create an empty repository named `distributed-job-scheduler` in GitHub, then run these commands locally from this project's root:

```bash
git init
git add .
git commit -m "Build PostgreSQL job scheduler with leased workers and tests"
git branch -M main
git remote add origin https://github.com/YashPulimi/distributed-job-scheduler.git
git push -u origin main
```

These are instructions for you; no repository has been created or pushed by this package. `.gitignore` excludes `.env`, caches, and local benchmark outputs. Add sanitized benchmark summaries deliberately if you want them in the repository. The supplied GitHub Actions workflow runs the full suite with PostgreSQL on pushes and pull requests. It has not been run remotely yet.

Add your own architecture explanation, a screenshot of a green CI run, a real benchmark summary, and a short crash-recovery demonstration to the README. Keep the validation limitations until you have replaced them with your own test evidence.

## 10. Update your resume after verification

Once you have run, understood, and extended the project, these bullets are a starting point. Use only features you can demonstrate:

- Built a PostgreSQL-backed job scheduler with a FastAPI API and concurrent worker processes, implementing priority scheduling, transactional job claims, and idempotent submissions.
- Implemented expiring worker leases, heartbeat renewal, capped exponential retries, and claim-token checks to recover abandoned jobs and reject stale result writes.
- Verified concurrency and recovery with PostgreSQL integration tests; measured throughput and p95 completion latency across one-slot and eight-slot configurations.

Only add numerical improvements from your actual benchmark files. Do not claim exactly-once execution, production scale, AWS deployment, or a green CI run until you have evidence for those claims. PostgreSQL, pytest, Git, and CI/CD become defensible skills when you have actually used and can explain them.

## Stop and troubleshoot

```bash
docker compose logs --tail=100 api worker db
docker compose down
```

`down` retains the named data volume. `docker compose down -v` also deletes this project's saved jobs and database; use it only when you deliberately want a fresh demo database.

| Symptom | Check |
|---|---|
| HTTP 401 | Match `X-API-Key` to the configured `API_KEY`; recreate containers after changing `.env` |
| HTTP 409 on submission | Reuse a key only for an identical request; use a new key for new work |
| HTTP 422 | Check payload type, priority bounds, and timezone in `scheduled_at` |
| Jobs remain queued | Check workers, future `available_at`, retry backoff, and database connectivity |
| Jobs repeatedly expire | Check worker crashes, lease length, and heartbeat/database errors |
| Tests show skips | Use the Compose test command with `TEST_DATABASE_URL` set by the service |
| API fails during startup | Check the migration exited 0 and PostgreSQL is healthy |
| Old code still runs | Rebuild with `docker compose up --build -d` after source edits |
