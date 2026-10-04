import os
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql

from scheduler.store import Store


@pytest.fixture
def store():
    dsn = os.getenv("TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("Set TEST_DATABASE_URL or run docker compose --profile test run --rm test")
    # A unique schema prevents tests from truncating application/user data.
    schema = "scheduler_test_" + uuid4().hex
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    store = Store(dsn, schema)
    try:
        store.initialize()
        yield store
    finally:
        with psycopg.connect(dsn, autocommit=True) as conn:
            conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))

