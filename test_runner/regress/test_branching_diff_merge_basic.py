from __future__ import annotations

import os
from collections import Counter
from typing import TYPE_CHECKING

import pytest
from fixtures.pg_version import PgVersion

if TYPE_CHECKING:
    import subprocess

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


def _start_endpoint(neon_env_builder: NeonEnvBuilder, branch_name: str) -> tuple[NeonEnv, Endpoint]:
    env = neon_env_builder.init_start()
    _skip_unless_opengauss(env)
    _allow_opengauss_basebackup_warnings(env)
    env.create_branch(branch_name)
    endpoint = env.endpoints.create_start(branch_name)
    endpoint.safe_psql("CREATE EXTENSION IF NOT EXISTS neon")
    return env, endpoint


def _branch_merge(
    env: NeonEnv,
    source_branch: str,
    target_branch: str,
    source_endpoint: Endpoint,
    target_endpoint: Endpoint,
    *,
    strategy: str = "fail",
    source_schema: str = "public",
    target_schema: str = "public",
    fdw_name: str = "neon_merge_src",
    check_return_code: bool = True,
) -> subprocess.CompletedProcess[str]:
    assert source_endpoint.endpoint_id is not None
    assert target_endpoint.endpoint_id is not None

    args = [
        "branch",
        "merge",
        "--tenant-id",
        str(env.initial_tenant),
        "--source-branch",
        source_branch,
        "--target-branch",
        target_branch,
        "--source-endpoint",
        source_endpoint.endpoint_id,
        "--target-endpoint",
        target_endpoint.endpoint_id,
        "--source-schema",
        source_schema,
        "--target-schema",
        target_schema,
        "--strategy",
        strategy,
        "--fdw-schema",
        fdw_name,
        "--fdw-server",
        fdw_name,
    ]
    return env.neon_cli.raw_cli(args, check_return_code=check_return_code)


def _branch_pair(
    neon_env_builder: NeonEnvBuilder, prefix: str
) -> tuple[NeonEnv, str, str, Endpoint, Endpoint]:
    env = neon_env_builder.init_start()
    _skip_unless_opengauss(env)
    _allow_opengauss_basebackup_warnings(env)

    target_branch = f"{prefix}_target"
    source_branch = f"{prefix}_source"
    endpoint_prefix = prefix.replace("_", "-")
    env.create_branch(target_branch)
    target = env.endpoints.create_start(target_branch, endpoint_id=f"{endpoint_prefix}-target")
    env.create_branch(source_branch, ancestor_branch_name=target_branch)
    source = env.endpoints.create_start(source_branch, endpoint_id=f"{endpoint_prefix}-source")
    return env, source_branch, target_branch, source, target


def test_branching_diff_reports_table_row_schema_and_pk_differences(
    neon_env_builder: NeonEnvBuilder,
):
    _, endpoint = _start_endpoint(neon_env_builder, "test_branch_diff_basic")

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
                balance integer,
                note text
            )
            """,
            """
            CREATE TABLE tgt.accounts (
                id integer PRIMARY KEY,
                name text,
                balance integer,
                note text
            )
            """,
            """
            INSERT INTO src.accounts VALUES
                (1, 'same', 10, NULL),
                (2, 'source', 20, 'changed'),
                (3, 'source-only', 30, NULL),
                (5, 'null-safe', 50, NULL)
            """,
            """
            INSERT INTO tgt.accounts VALUES
                (1, 'same', 10, NULL),
                (2, 'target', 15, 'old'),
                (4, 'target-only', 40, NULL),
                (5, 'null-safe', 50, NULL)
            """,
            "CREATE TABLE src.source_only_table (id integer PRIMARY KEY, value text)",
            "INSERT INTO src.source_only_table VALUES (1, 'from-source')",
            "CREATE TABLE tgt.target_only_table (id integer PRIMARY KEY, value text)",
            "INSERT INTO tgt.target_only_table VALUES (1, 'from-target')",
            "CREATE TABLE src.schema_mismatch (id integer PRIMARY KEY, value text)",
            "CREATE TABLE tgt.schema_mismatch (id integer PRIMARY KEY, value integer)",
            "CREATE TABLE src.no_pk (id integer, value text)",
            "CREATE TABLE tgt.no_pk (id integer, value text)",
            """
            CREATE TABLE src.composite_pk (
                tenant_id integer,
                id integer,
                value text,
                PRIMARY KEY (tenant_id, id)
            )
            """,
            """
            CREATE TABLE tgt.composite_pk (
                tenant_id integer,
                id integer,
                value text,
                PRIMARY KEY (tenant_id, id)
            )
            """,
            """
            INSERT INTO src.composite_pk VALUES
                (1, 1, 'same'),
                (1, 2, 'source-conflict'),
                (1, 3, 'source-only')
            """,
            """
            INSERT INTO tgt.composite_pk VALUES
                (1, 1, 'same'),
                (1, 2, 'target-conflict'),
                (1, 4, 'target-only')
            """,
        ]
    )

    rows = endpoint.safe_psql(
        """
        SELECT table_name, diff_type
        FROM neon_branch_diff('src'::name, 'tgt'::name, NULL::name[])
        ORDER BY table_name, diff_type, row_data
        """
    )
    counts = Counter(rows)

    assert counts[("accounts", "conflict")] == 1
    assert counts[("accounts", "source_only")] == 1
    assert counts[("accounts", "target_only")] == 1
    assert counts[("source_only_table", "source_only")] == 1
    assert counts[("target_only_table", "target_only")] == 1
    assert counts[("schema_mismatch", "schema_mismatch")] == 1
    assert counts[("no_pk", "no_primary_key")] == 1
    assert counts[("composite_pk", "conflict")] == 1
    assert counts[("composite_pk", "source_only")] == 1
    assert counts[("composite_pk", "target_only")] == 1

    filtered = endpoint.safe_psql(
        """
        SELECT diff_type, count(*)
        FROM neon_branch_diff('src'::name, 'tgt'::name, ARRAY['accounts']::name[])
        GROUP BY diff_type
        ORDER BY diff_type
        """
    )
    assert filtered == [("conflict", 1), ("source_only", 1), ("target_only", 1)]


def test_branching_merge_strategies_counts_and_idempotence(neon_env_builder: NeonEnvBuilder):
    _, endpoint = _start_endpoint(neon_env_builder, "test_branch_merge_strategies")

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
        ]
    )

    def reset_rows() -> None:
        endpoint.safe_psql_many(
            [
                "TRUNCATE src.accounts",
                "TRUNCATE tgt.accounts",
                """
                INSERT INTO src.accounts VALUES
                    (1, 'same', 10),
                    (2, 'source', 20),
                    (3, 'source-only', 30)
                """,
                """
                INSERT INTO tgt.accounts VALUES
                    (1, 'same', 10),
                    (2, 'target', 15),
                    (4, 'target-only', 40)
                """,
            ]
        )

    reset_rows()
    with pytest.raises(Exception, match="merge conflict in table"):
        endpoint.safe_psql(
            """
            SELECT *
            FROM neon_branch_merge(
                'src'::name, 'tgt'::name, 'fail', ARRAY['accounts']::name[], true
            )
            """
        )
    assert endpoint.safe_psql("SELECT * FROM tgt.accounts ORDER BY id") == [
        (1, "same", 10),
        (2, "target", 15),
        (4, "target-only", 40),
    ]

    reset_rows()
    assert endpoint.safe_psql(
        """
        SELECT table_name, inserted_count, updated_count
        FROM neon_branch_merge(
            'src'::name, 'tgt'::name, 'ours', ARRAY['accounts']::name[], true
        )
        """
    ) == [("accounts", 1, 0)]
    assert endpoint.safe_psql("SELECT * FROM tgt.accounts ORDER BY id") == [
        (1, "same", 10),
        (2, "target", 15),
        (3, "source-only", 30),
        (4, "target-only", 40),
    ]

    reset_rows()
    assert endpoint.safe_psql(
        """
        SELECT table_name, inserted_count, updated_count
        FROM neon_branch_merge(
            'src'::name, 'tgt'::name, 'theirs', ARRAY['accounts']::name[], true
        )
        """
    ) == [("accounts", 1, 1)]
    assert endpoint.safe_psql("SELECT * FROM tgt.accounts ORDER BY id") == [
        (1, "same", 10),
        (2, "source", 20),
        (3, "source-only", 30),
        (4, "target-only", 40),
    ]
    assert endpoint.safe_psql(
        """
        SELECT table_name, inserted_count, updated_count
        FROM neon_branch_merge(
            'src'::name, 'tgt'::name, 'theirs', ARRAY['accounts']::name[], true
        )
        """
    ) == [("accounts", 0, 0)]

    with pytest.raises(Exception, match="invalid merge strategy"):
        endpoint.safe_psql(
            """
            SELECT *
            FROM neon_branch_merge(
                'src'::name, 'tgt'::name, 'invalid', ARRAY['accounts']::name[], true
            )
            """
        )


def test_branching_merge_cli_copies_source_only_table_schema_and_data(
    neon_env_builder: NeonEnvBuilder,
):
    env, source_branch, target_branch, source, target = _branch_pair(
        neon_env_builder, "bdm_copy"
    )

    source.safe_psql_many(
        [
            "CREATE SCHEMA copy_src",
            """
            CREATE TABLE copy_src.source_only_copy (
                id integer NOT NULL,
                name text NOT NULL DEFAULT 'n/a',
                amount integer CONSTRAINT amount_positive CHECK (amount > 0),
                tag text,
                CONSTRAINT source_only_copy_pkey PRIMARY KEY (id),
                CONSTRAINT source_only_copy_tag_key UNIQUE (tag)
            )
            """,
            "CREATE INDEX source_only_copy_amount_idx ON copy_src.source_only_copy (amount)",
            """
            INSERT INTO copy_src.source_only_copy (id, amount, tag) VALUES
                (1, 10, 'a'),
                (2, 20, 'b')
            """,
        ]
    )

    result = _branch_merge(
        env,
        source_branch,
        target_branch,
        source,
        target,
        source_schema="copy_src",
        target_schema="copy_tgt",
        fdw_name="fdw_copy_src",
    )
    assert "copy_tgt.source_only_copy\tinserted=2\tupdated=0" in result.stdout

    assert target.safe_psql("SELECT * FROM copy_tgt.source_only_copy ORDER BY id") == [
        (1, "n/a", 10, "a"),
        (2, "n/a", 20, "b"),
    ]
    assert target.safe_psql(
        """
        SELECT attname, attnotnull
        FROM pg_attribute
        WHERE attrelid = 'copy_tgt.source_only_copy'::regclass
          AND attname IN ('id', 'name')
        ORDER BY attname
        """
    ) == [("id", True), ("name", True)]
    default_expr = target.safe_psql(
        """
        SELECT pg_get_expr(d.adbin, d.adrelid)::text
        FROM pg_attrdef d
        JOIN pg_attribute a ON a.attrelid = d.adrelid AND a.attnum = d.adnum
        WHERE d.adrelid = 'copy_tgt.source_only_copy'::regclass
          AND a.attname = 'name'
        """
    )[0][0]
    assert "n/a" in default_expr
    assert target.safe_psql(
        """
        SELECT contype, count(*)
        FROM pg_constraint
        WHERE conrelid = 'copy_tgt.source_only_copy'::regclass
        GROUP BY contype
        ORDER BY contype
        """
    ) == [("c", 1), ("p", 1), ("u", 1)]
    assert target.safe_psql(
        """
        SELECT count(*)
        FROM pg_class i
        JOIN pg_index ix ON ix.indexrelid = i.oid
        WHERE ix.indrelid = 'copy_tgt.source_only_copy'::regclass
          AND i.relname = 'source_only_copy_amount_idx'
        """
    ) == [(1,)]


UNSUPPORTED_SOURCE_ONLY_CASES = [
    (
        "view",
        "CREATE SCHEMA {schema}; CREATE VIEW {schema}.v AS SELECT 1 AS id",
        "views",
    ),
    (
        "function",
        """
        CREATE SCHEMA {schema};
        CREATE FUNCTION {schema}.f() RETURNS integer LANGUAGE sql AS 'SELECT 1'
        """,
        "functions or procedures",
    ),
    (
        "sequence",
        "CREATE SCHEMA {schema}; CREATE SEQUENCE {schema}.s",
        "sequences",
    ),
    (
        "foreign_key",
        """
        CREATE SCHEMA {schema};
        CREATE TABLE {schema}.parent (id integer PRIMARY KEY);
        CREATE TABLE {schema}.child (
            id integer PRIMARY KEY,
            parent_id integer REFERENCES {schema}.parent(id)
        )
        """,
        "foreign key constraints",
    ),
    (
        "comment",
        """
        CREATE SCHEMA {schema};
        CREATE TABLE {schema}.t (id integer PRIMARY KEY);
        COMMENT ON TABLE {schema}.t IS 'not copied'
        """,
        "table or column comments",
    ),
    (
        "privileges",
        """
        CREATE SCHEMA {schema};
        CREATE TABLE {schema}.t (id integer PRIMARY KEY);
        GRANT SELECT ON {schema}.t TO PUBLIC
        """,
        "owner or explicit privileges",
    ),
    (
        "trigger",
        """
        CREATE OR REPLACE FUNCTION public.bdm_trigger_fn()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            RETURN NEW;
        END;
        $$;
        CREATE SCHEMA {schema};
        CREATE TABLE {schema}.t (id integer PRIMARY KEY);
        CREATE TRIGGER t_block BEFORE INSERT ON {schema}.t
        FOR EACH ROW EXECUTE PROCEDURE public.bdm_trigger_fn()
        """,
        "triggers",
    ),
]


def test_branching_merge_cli_rejects_unsupported_source_only_objects(
    neon_env_builder: NeonEnvBuilder,
):
    env, source_branch, target_branch, source, target = _branch_pair(neon_env_builder, "bdm_bad")

    for case_name, setup_sql, expected_error in UNSUPPORTED_SOURCE_ONLY_CASES:
        source_schema = f"src_bad_{case_name}"
        target_schema = f"tgt_bad_{case_name}"
        source.safe_psql(setup_sql.format(schema=source_schema), log_query=False)

        result = _branch_merge(
            env,
            source_branch,
            target_branch,
            source,
            target,
            source_schema=source_schema,
            target_schema=target_schema,
            fdw_name=f"fdw_bad_{case_name}",
            check_return_code=False,
        )

        assert result.returncode != 0, case_name
        assert expected_error in result.stderr, result.stderr
        assert target.safe_psql(
            f"""
            SELECT count(*)
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = '{target_schema}'
              AND c.relkind = 'r'
            """
        ) == [(0,)]
