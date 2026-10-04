import json
import logging
import signal
import socket
import threading
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

from psycopg import Error as DatabaseError

from scheduler.config import Settings
from scheduler.handlers import LeaseLost, execute
from scheduler.store import Store

log = logging.getLogger("scheduler.worker")


def event(name, **fields):
    log.info(json.dumps({"event": name, **fields}, default=str))


def run_claim(store, job, lease_seconds=15, handler=execute):
    """Heartbeat independently of the handler; fence every final state write."""
    done, lease_lost = threading.Event(), threading.Event()

    def heartbeat():
        while not done.wait(lease_seconds / 3):
            try:
                if not store.heartbeat(job, lease_seconds):
                    lease_lost.set()
                    return
            except DatabaseError:
                # Be conservative: stop cooperative work; the lease will recover it.
                lease_lost.set()
                event("heartbeat_database_error", job_id=job["id"])
                return

    thread = threading.Thread(target=heartbeat, daemon=True)
    thread.start()
    try:
        try:
            result = handler(job, lease_lost)
        except LeaseLost:
            event("lease_lost", job_id=job["id"])
            return False
        except Exception as exc:
            accepted = store.fail(job, f"{type(exc).__name__}: {exc}")
            event("attempt_failed", job_id=job["id"], attempt=job["attempt_count"], accepted=accepted)
            return accepted
        if lease_lost.is_set():
            return False
        accepted = store.succeed(job, result)
        event("attempt_completed", job_id=job["id"], attempt=job["attempt_count"], accepted=accepted)
        return accepted
    finally:
        done.set()
        thread.join(timeout=6)


def loop(store, worker_id, stop, settings):
    while not stop.is_set():
        try:
            recovered = store.recover_expired()
            if recovered:
                event("leases_recovered", count=recovered)
            job = store.claim(worker_id, settings.lease_seconds)
            if job:
                event("job_claimed", job_id=job["id"], worker_id=worker_id, attempt=job["attempt_count"])
                run_claim(store, job, settings.lease_seconds)
                continue
        except DatabaseError:
            # Do not print the DSN or connection exception: it may contain credentials.
            event("database_unavailable", worker_id=worker_id)
        stop.wait(settings.poll_seconds)


def main():
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    settings = Settings.from_env()
    store = Store(settings.database_url, settings.schema, settings.aging_seconds)
    store.ping()
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    worker_id = f"{socket.gethostname()}-{uuid4().hex[:8]}"
    event("worker_started", worker_id=worker_id, concurrency=settings.concurrency)
    with ThreadPoolExecutor(max_workers=settings.concurrency) as pool:
        futures = [pool.submit(loop, store, f"{worker_id}/{i}", stop, settings)
                   for i in range(settings.concurrency)]
        for future in futures:
            future.result()


if __name__ == "__main__":
    main()
