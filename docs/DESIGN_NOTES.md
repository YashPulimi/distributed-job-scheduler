# Design decisions to explain in an interview

## Why this project fits your background

Your existing projects show document processing, APIs, and AWS usage. This scheduler adds focused practice with transactions, concurrent work, failure recovery, and tests. It also gives you a service you could later use to run document-processing jobs. Keep that integration as future work until implemented.

## Why PostgreSQL instead of Redis plus another database

A single database makes submission idempotency, claim state, attempt history, and guarded completion easier to reason about transactionally. It keeps the first version small enough to understand. Redis or a managed broker could improve other workload tradeoffs, but adding one introduces additional coordination and failure boundaries.

This is not evidence that PostgreSQL is universally the best queue. Queue scans with dynamic aging and per-operation connections impose costs. Measure the actual workload before adding infrastructure.

## Why not use an in-memory heap as the distributed queue

A heap is useful for a scheduler in one process. If every worker owns its own heap, they can disagree about job state and lose data on process failure. Here, the durable shared table is authoritative. The ordering query implements priority selection and aging without pretending the implementation is a binary heap or has logarithmic dequeue complexity.

To practice heap data structures separately, implement a local-only queue and compare its limitations. Do not replace the database transaction with a process-local mutex and assume multiple containers are coordinated.

## Why leases and claim tokens are both needed

A lease lets the system eventually recover work when a worker stops responding. A claim token distinguishes the current owner from older attempts. Without the token check, a paused worker could wake up after reassignment and overwrite a newer result. Both validity checks happen inside the database UPDATE that writes the result.

Tokens in this implementation are random attempt identifiers, not monotonic fencing numbers for external storage. They protect the scheduler's database state only.

## Why the service cannot promise exactly-once execution

Suppose a handler sends a notification and then crashes before committing success. Retrying can send the notification again. The scheduler cannot infer whether the external action occurred. A submission idempotency key solves a different problem: an API client repeating the same POST.

For external work, use downstream idempotency based on job ID, or a carefully designed transactional outbox. Explain this boundary directly rather than claiming zero duplicates.

## What fairness means here

An eligible job gains one priority point for every AGING_SECONDS spent waiting. Jobs with equal effective priority use their submission sequence as a tie-breaker. An old low-priority job can outrank a newly submitted high-priority job. Retried jobs begin waiting again after backoff.

SKIP LOCKED makes claim preference approximate under contention. Multiple workers finish at different times. Aging helps prevent perpetual overtaking by new high-priority arrivals but cannot manufacture capacity in an overloaded system.

## What to measure next

Record end-to-end throughput and p95 completion latency with the same synthetic workload and changing only slot count. Next, measure database connection overhead and claim-query plans. Consider a connection pool and smaller indexed priority buckets if measurements justify them. Separate synthetic sleep-workload results from CPU-heavy workloads.

## Questions you should be able to answer

1. What happens if the API client times out after the INSERT commits?
2. Can two workers claim one row at the same instant?
3. What happens if a worker loses database connectivity while still executing?
4. What happens if the process dies after computing a result but before committing it?
5. Why is queued cancellation safe but running cancellation deliberately excluded?
6. Which test would detect a stale worker overwriting a newer attempt?
7. Why does increasing worker count eventually stop improving throughput?
8. Which changes are needed before exposing the API to multiple tenants?
