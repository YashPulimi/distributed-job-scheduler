import secrets
from contextlib import asynccontextmanager
from typing import Annotated, Literal
from uuid import UUID

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Response
from pydantic import ValidationError
from psycopg import Error as DatabaseError

from scheduler.config import Settings
from scheduler.models import JobRequest, PAYLOAD_TYPES
from scheduler.store import IdempotencyConflict, Store


def public_job(row):
    # Claim credentials and request fingerprints are worker/storage internals.
    return {k: v for k, v in row.items() if k not in {"claim_token", "request_hash", "idempotency_key"}}


def create_app(settings=None, store=None):
    settings = settings or Settings.from_env()
    store = store or Store(settings.database_url, settings.schema, settings.aging_seconds)

    @asynccontextmanager
    async def lifespan(app):
        store.ping()
        yield

    app = FastAPI(title="Distributed Job Scheduler", version="0.1.0", lifespan=lifespan)

    def authorize(x_api_key: Annotated[str | None, Header()] = None):
        if x_api_key is None or not secrets.compare_digest(x_api_key.encode(), settings.api_key.encode()):
            raise HTTPException(401, "Missing or invalid X-API-Key")

    auth = [Depends(authorize)]

    @app.get("/health")
    def health():
        try:
            store.ping()
        except DatabaseError:
            raise HTTPException(503, "Database unavailable") from None
        return {"status": "ok"}

    @app.post("/jobs", status_code=201, dependencies=auth)
    def submit(request: JobRequest, response: Response,
               idempotency_key: Annotated[str | None, Header(min_length=1, max_length=128)] = None):
        try:
            payload = PAYLOAD_TYPES[request.kind].model_validate(request.payload).model_dump(mode="json")
        except ValidationError as exc:
            raise HTTPException(422, exc.errors(include_context=False, include_url=False, include_input=False)) from None
        spec = request.model_dump(mode="json")
        spec["payload"] = payload
        try:
            job, created = store.submit(spec, idempotency_key)
        except IdempotencyConflict as exc:
            raise HTTPException(409, str(exc)) from None
        response.status_code = 201 if created else 200
        response.headers["Location"] = f"/jobs/{job['id']}"
        return public_job(job)

    @app.get("/jobs", dependencies=auth)
    def list_jobs(
        state: Literal["queued", "running", "succeeded", "failed", "cancelled"] | None = None,
        limit: int = Query(default=50, ge=1, le=100),
    ):
        return [public_job(j) for j in store.list(state, limit)]

    @app.get("/jobs/{job_id}", dependencies=auth)
    def get_job(job_id: UUID):
        job = store.get(job_id)
        if not job:
            raise HTTPException(404, "Job not found")
        return public_job(job)

    @app.get("/jobs/{job_id}/attempts", dependencies=auth)
    def history(job_id: UUID):
        if not store.get(job_id):
            raise HTTPException(404, "Job not found")
        return [{k: v for k, v in a.items() if k != "claim_token"} for a in store.history(job_id)]

    @app.post("/jobs/{job_id}/cancel", dependencies=auth)
    def cancel(job_id: UUID):
        job = store.cancel(job_id)
        if job:
            return public_job(job)
        if not store.get(job_id):
            raise HTTPException(404, "Job not found")
        raise HTTPException(409, "Only queued jobs can be cancelled")

    @app.get("/stats", dependencies=auth)
    def stats():
        return store.stats()

    return app


app = create_app()
