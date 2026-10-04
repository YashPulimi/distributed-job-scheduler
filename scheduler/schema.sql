CREATE TABLE IF NOT EXISTS jobs (
    id UUID PRIMARY KEY,
    sequence BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
    idempotency_key TEXT UNIQUE,
    request_hash TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('sum', 'word_count', 'demo')),
    payload JSONB NOT NULL,
    priority INTEGER NOT NULL CHECK (priority BETWEEN 0 AND 10),
    state TEXT NOT NULL DEFAULT 'queued'
        CHECK (state IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')),
    max_attempts INTEGER NOT NULL CHECK (max_attempts BETWEEN 1 AND 10),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    available_at TIMESTAMPTZ NOT NULL,
    enqueued_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    finished_at TIMESTAMPTZ,
    claim_token UUID,
    worker_id TEXT,
    lease_expires_at TIMESTAMPTZ,
    result JSONB,
    last_error TEXT,
    CHECK (attempt_count <= max_attempts),
    CHECK ((state = 'running') =
        (claim_token IS NOT NULL AND worker_id IS NOT NULL AND lease_expires_at IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS jobs_ready_idx ON jobs (available_at, sequence) WHERE state = 'queued';
CREATE INDEX IF NOT EXISTS jobs_expired_idx ON jobs (lease_expires_at) WHERE state = 'running';
CREATE INDEX IF NOT EXISTS jobs_state_idx ON jobs (state, sequence DESC);

CREATE TABLE IF NOT EXISTS attempts (
    job_id UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    number INTEGER NOT NULL,
    claim_token UUID UNIQUE NOT NULL,
    worker_id TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    finished_at TIMESTAMPTZ,
    outcome TEXT NOT NULL DEFAULT 'running'
        CHECK (outcome IN ('running', 'succeeded', 'retry', 'failed', 'lease_expired')),
    error TEXT,
    PRIMARY KEY (job_id, number)
);

