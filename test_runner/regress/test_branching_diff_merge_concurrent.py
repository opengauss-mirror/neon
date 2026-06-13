from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING

import pytest
from fixtures.pg_version import PgVersion
from fixtures.utils import wait_until

if TYPE_CHECKING:
    from fixtures.neon_fixtures import Endpoint, NeonEnv, NeonEnvBuilder

pytestmark = pytest.mark.skipif(
    not PgVersion(os.getenv("DEFAULT_PG_VERSION", PgVersion.DEFAULT)).is_opengauss,
    reason="branch diff/merge extension tests are openGauss-specific",
)

OPEN_GAUSS_BASEBACKUP_ALLOWED_ERRORS = [
    ".*basebackup: add pg_(type|class_oid_index|class_tblspc_relfilenode_index) failed .*could not find data for key.*",
]
OPEN_GAUSS_STORAGE_CONTROLLER_ALLOWED_ERRORS = [
    ".*Shared lock by TimelineCreate was held for .*",
]


def _skip_unless_opengauss(env: NeonEnv) -> None:
    if not env.pg_version.is_opengauss:
        pytest.skip("branch diff/merge extension tests are openGauss-specific")


def _allow_opengauss_basebackup_warnings(env: NeonEnv) -> None:
    env.pageserver.allowed_errors.extend(OPEN_GAUSS_BASEBACKUP_ALLOWED_ERRORS)
    env.storage_controller.allowed_errors.extend(OPEN_GAUSS_STORAGE_CONTROLLER_ALLOWED_ERRORS)


def _start_endpoint(neon_env_builder: NeonEnvBuilder) -> Endpoint:
    env = neon_env_builder.init_start()
    _skip_unless_opengauss(env)
    _allow_opengauss_basebackup_warnings(env)
    env.create_branch("test_branch_merge_concurrent")
    endpoint = env.endpoints.create_start("test_branch_merge_concurrent")
    endpoint.safe_psql("CREATE EXTENSION IF NOT EXISTS neon")
    return endpoint


def test_branching_merge_locks_target_table_and_blocks_concurrent_writes(
    neon_env_builder: NeonEnvBuilder,
):
    endpoint = _start_endpoint(neon_env_builder)

    endpoint.safe_psql_many(
        [
            "DROP SCHEMA IF EXISTS src CASCADE",
            "DROP SCHEMA IF EXISTS tgt CASCADE",
            "CREATE SCHEMA src",
            "CREATE SCHEMA tgt",
            """
            CREATE TABLE src.accounts (
                id integer PRIMARY KEY,
                name text,
                balance integer
            )
            """,
            """
            CREATE TABLE tgt.accounts (
                id integer PRIMARY KEY,
                name text,
                balance integer
            )
            """,
            "INSERT INTO src.accounts VALUES (1, 'source', 20), (2, 'new', 30)",
            "INSERT INTO tgt.accounts VALUES (1, 'target', 10)",
            """
            CREATE OR REPLACE FUNCTION tgt.slow_before_update()
            RETURNS trigger
            LANGUAGE plpgsql
            AS $$
            BEGIN
                PERFORM pg_sleep(3);
                RETURN NEW;
            END;
            $$
            """,
            """
            CREATE TRIGGER accounts_slow_before_update
            BEFORE UPDATE ON tgt.accounts
            FOR EACH ROW EXECUTE PROCEDURE tgt.slow_before_update()
            """,
        ]
    )

    def run_merge() -> list[tuple[str, int, int]]:
        return endpoint.safe_psql(
            """
            SELECT table_name, inserted_count, updated_count
            FROM neon_branch_merge(
                'src'::name, 'tgt'::name, 'theirs', ARRAY['accounts']::name[], true
            )
            """
        )

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(run_merge)

        def merge_lock_is_visible() -> None:
            rows = endpoint.safe_psql(
                """
                SELECT 1
                FROM pg_locks
                WHERE relation = 'tgt.accounts'::regclass
                  AND mode = 'ShareRowExclusiveLock'
                  AND granted
                LIMIT 1
                """
            )
            assert rows

        wait_until(merge_lock_is_visible, timeout=10, interval=0.1)

        with endpoint.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SET lockwait_timeout = 1000")
                with pytest.raises(Exception) as excinfo:
                    cur.execute("INSERT INTO tgt.accounts VALUES (99, 'blocked', 99)")

        error_text = str(excinfo.value).lower()
        assert "lock" in error_text or "timeout" in error_text
        assert future.result(timeout=10) == [("accounts", 1, 1)]

    assert endpoint.safe_psql("SELECT * FROM tgt.accounts ORDER BY id") == [
        (1, "source", 20),
        (2, "new", 30),
    ]
