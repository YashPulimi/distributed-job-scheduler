import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    database_url: str = "postgresql://scheduler:scheduler@localhost:5432/scheduler"
    api_key: str = "local-development-only"
    schema: str = "public"
    concurrency: int = 4
    lease_seconds: float = 15
    poll_seconds: float = 0.25
    aging_seconds: int = 30

    def __post_init__(self):
        if not self.api_key or not 1 <= self.concurrency <= 32:
            raise ValueError("API_KEY must be nonempty; WORKER_CONCURRENCY must be 1..32")
        if self.lease_seconds < 2 or self.poll_seconds <= 0 or self.aging_seconds < 1:
            raise ValueError("Lease must be >=2s; polling and aging intervals must be positive")

    @classmethod
    def from_env(cls):
        return cls(
            database_url=os.getenv("DATABASE_URL", cls.database_url),
            api_key=os.getenv("API_KEY", cls.api_key),
            schema=os.getenv("SCHEDULER_SCHEMA", cls.schema),
            concurrency=int(os.getenv("WORKER_CONCURRENCY", "4")),
            lease_seconds=float(os.getenv("LEASE_SECONDS", "15")),
            poll_seconds=float(os.getenv("POLL_SECONDS", "0.25")),
            aging_seconds=int(os.getenv("AGING_SECONDS", "30")),
        )

