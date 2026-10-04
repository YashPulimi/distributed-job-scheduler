# Validation record

Prepared on October 4, 2026 UTC (October 3 in America/New_York).

## Executed successfully

- Python 3.12 source compilation for the application, tests, and scripts.
- **23 passing test cases** for handlers, retry policy, bounded payload validation, timezone validation, configuration checks, API authentication, and invalid HTTP requests.
- **20 PostgreSQL statements parsed** with PostgreSQL's grammar through `pglast`, including the schema and repository queries. Parameters were replaced with positional PostgreSQL parameters for parsing. This proves syntax acceptance, not database behavior, type resolution, or query performance.
- Compose YAML and GitHub Actions YAML parsed successfully. This does not substitute for a Docker Compose build/run.
- Benchmark command-line help loaded successfully.

The test runner emitted one upstream Starlette deprecation warning for its HTTPX TestClient compatibility path. The affected API-validation test passed with the locked dependencies.

## Not executed in the preparation environment

- **14 PostgreSQL integration cases were skipped** because a PostgreSQL test server could not be started in this restricted environment and Docker is unavailable.
- Docker image build, Compose orchestration, end-to-end demo, crash-recovery experiment, performance benchmark, and GitHub Actions execution have not run here.
- No throughput, p95 latency, scale, AWS deployment, or production reliability result has been measured or claimed.

A native PostgreSQL setup was attempted for validation, but the environment could not support the necessary non-root server setup. The project does not depend on that attempted setup; its documented runtime is Docker Compose with PostgreSQL 17.

## Required local gate

Run from the project root:

```bash
docker compose up --build -d
docker compose --profile test run --build --rm test
docker compose exec api python scripts/demo.py
```

Require **37 passing tests and zero skips**, then a passing demo. The database tests use isolated temporary schemas rather than deleting your application data. If the suite fails, retain the failure details and fix the cause before claiming the feature works.

The integration suite covers priority/FIFO selection, age-based promotion, future scheduling, simultaneous idempotent submissions, eight claimers draining 40 jobs, skipping row locks, retries and terminal failures, expired-worker recovery, stale-result rejection, cancellation, heartbeat renewal, handler persistence, and API behavior against PostgreSQL.

Then run the crash and benchmark exercises in STEP_BY_STEP.md. Replace this validation record with your actual machine/CI output when available. The initial numbers above describe checks run while preparing the code, not a benchmark of your implementation.

