"""
Prefetch 数据正确性测试
验证预取操作不会导致数据错误
"""
from __future__ import annotations

import time
from typing import TYPE_CHECKING

import pytest
from fixtures.utils import USE_LFC

if TYPE_CHECKING:
    from fixtures.neon_fixtures import NeonEnv


def _restart_endpoint(env, endpoint, conn):
    """Restart compute to guarantee shared_buffers is cleared, forcing reads through LFC/prefetch.

    Uses mode="immediate" with sks_wait_walreceiver_gone so the old walreceiver
    disconnects from the safekeeper before the new endpoint does sync-safekeepers.
    mode="fast" is not usable here because openGauss gs_ctl has a very short
    default shutdown wait timeout and fails with "server does not shut down".

    A CHECKPOINT is issued first to ensure all WAL is flushed to the safekeeper
    before the process is killed.
    """
    cur = conn.cursor()
    cur.execute("CHECKPOINT")
    cur.close()
    conn.close()
    time.sleep(1)
    endpoint.stop(
        mode="immediate",
        sks_wait_walreceiver_gone=(env.safekeepers, env.initial_timeline),
    )
    endpoint.start()
    conn = endpoint.connect()
    return conn, conn.cursor()


def _explain_plan(cur, query: str) -> str:
    cur.execute(f"EXPLAIN (COSTS OFF) {query}")
    return "\n".join(row[0] for row in cur.fetchall())


@pytest.mark.skipif(not USE_LFC, reason="LFC is disabled, skipping")
def test_prefetch_index_scan_correctness(neon_simple_env: NeonEnv):
    """
    验证索引扫描预取的数据正确性
    
    步骤：
    1. 创建表和索引
    2. 执行索引范围查询（触发预取）
    3. 验证查询结果与预期一致
    """
    env = neon_simple_env
    endpoint = env.endpoints.create_start(
        "main",
        config_lines=[
            "neon.max_file_cache_size=64MB",
            "neon.file_cache_size_limit=64MB",
            "shared_buffers=1MB",
            "enable_seqscan=off",
            "enable_bitmapscan=off",
            "enable_indexscan=on",
        ],
    )
    conn = endpoint.connect()
    cur = conn.cursor()
    
    cur.execute("CREATE TABLE test_index_prefetch (id int4 PRIMARY KEY, value int, data text default repeat('x', 100))")
    cur.execute("CREATE INDEX idx_value ON test_index_prefetch(value)")
    
    n_rows = 10000
    cur.execute(
        f"INSERT INTO test_index_prefetch (id, value) "
        f"SELECT g, (g * 7) % 100000 FROM generate_series(1, {n_rows}) g"
    )
    cur.execute("ANALYZE test_index_prefetch")
    
    low = 10000
    high = 20000
    query = (
        f"SELECT count(*), sum(id), min(value), max(value) "
        f"FROM test_index_prefetch WHERE value BETWEEN {low} AND {high}"
    )
    plan = _explain_plan(cur, query)
    assert "Index Scan" in plan, f"Expected index scan plan, got:\n{plan}"

    cur.execute(query)
    result = cur.fetchone()
    count = result[0]
    sum_id = result[1]
    min_val = result[2]
    max_val = result[3]
    
    assert count > 0, "Should have some rows in range"
    assert min_val >= low, f"Min value {min_val} should be >= {low}"
    assert max_val <= high, f"Max value {max_val} should be <= {high}"
    
    conn, cur = _restart_endpoint(env, endpoint, conn)
    
    cur.execute(query)
    result2 = cur.fetchone()
    
    assert result2[0] == count, f"Count mismatch: {result2[0]} vs {count}"
    assert result2[1] == sum_id, f"Sum mismatch: {result2[1]} vs {sum_id}"
    assert result2[2] == min_val, f"Min mismatch: {result2[2]} vs {min_val}"
    assert result2[3] == max_val, f"Max mismatch: {result2[3]} vs {max_val}"


@pytest.mark.skipif(not USE_LFC, reason="LFC is disabled, skipping")
def test_prefetch_bitmap_scan_correctness(neon_simple_env: NeonEnv):
    """
    验证位图扫描预取的数据正确性

    使用 OR 条件跨两个独立索引列查询，迫使优化器选择 BitmapOr 计划。
    """
    env = neon_simple_env
    endpoint = env.endpoints.create_start(
        "main",
        config_lines=[
            "neon.max_file_cache_size=64MB",
            "neon.file_cache_size_limit=64MB",
            "shared_buffers=1MB",
            "enable_seqscan=off",
            "enable_indexscan=off",
            "enable_bitmapscan=on",
        ],
    )
    conn = endpoint.connect()
    cur = conn.cursor()

    cur.execute("SET enable_seqscan = off")
    cur.execute("SET enable_indexscan = off")

    cur.execute("""
        CREATE TABLE test_bitmap_prefetch (
            id int4 PRIMARY KEY,
            category int4,
            status int4,
            value numeric,
            data text default repeat('y', 200)
        )
    """)
    cur.execute("CREATE INDEX idx_category ON test_bitmap_prefetch(category)")
    cur.execute("CREATE INDEX idx_status ON test_bitmap_prefetch(status)")

    n_rows = 50000
    cur.execute(
        f"INSERT INTO test_bitmap_prefetch (id, category, status, value) "
        f"SELECT g, g % 50, g % 30, g * 1.5 FROM generate_series(1, {n_rows}) g"
    )
    cur.execute("ANALYZE test_bitmap_prefetch")

    cat_a, cat_b = 7, 23
    status_val = 5
    query = (
        f"SELECT count(*), sum(value) FROM test_bitmap_prefetch "
        f"WHERE category = {cat_a} OR category = {cat_b} OR status = {status_val}"
    )
    plan = _explain_plan(cur, query)
    assert "Bitmap" in plan, f"Expected Bitmap plan, got:\n{plan}"

    cur.execute(query)
    result = cur.fetchone()
    count = result[0]
    sum_value = result[1]
    assert count > 0, "Expected non-empty bitmap result set"

    conn, cur = _restart_endpoint(env, endpoint, conn)

    cur.execute("SET enable_seqscan = off")
    cur.execute("SET enable_indexscan = off")

    cur.execute(query)
    result2 = cur.fetchone()

    assert result2[0] == count, f"Count mismatch: {result2[0]} vs {count}"
    assert result2[1] == sum_value, f"Sum mismatch: {result2[1]} vs {sum_value}"

    verify_query = (
        f"SELECT id, category, status FROM test_bitmap_prefetch "
        f"WHERE category = {cat_a} OR category = {cat_b} OR status = {status_val} "
        f"ORDER BY id"
    )
    cur.execute(verify_query)
    rows = cur.fetchall()

    for row_id, cat, st in rows:
        assert cat == cat_a or cat == cat_b or st == status_val, (
            f"Row id={row_id} does not match filter: category={cat}, status={st}"
        )


@pytest.mark.skipif(not USE_LFC, reason="LFC is disabled, skipping")
def test_prefetch_seq_scan_correctness(neon_simple_env: NeonEnv):
    """
    验证顺序扫描预取的数据正确性
    
    步骤：
    1. 创建大表
    2. 执行全表扫描（触发顺序预取）
    3. 验证扫描结果完整且正确
    """
    env = neon_simple_env
    endpoint = env.endpoints.create_start(
        "main",
        config_lines=[
            "neon.max_file_cache_size=64MB",
            "neon.file_cache_size_limit=64MB",
            "shared_buffers=1MB",
            "enable_indexscan=off",
            "enable_bitmapscan=off",
        ],
    )
    conn = endpoint.connect()
    cur = conn.cursor()
    
    cur.execute("""
        CREATE TABLE test_seq_prefetch (
            id int4,
            grp int,
            value numeric,
            data text default repeat('z', 200)
        )
    """)
    
    n_rows = 20000
    cur.execute(
        f"INSERT INTO test_seq_prefetch (id, grp, value) "
        f"SELECT g, g % 100, g * 2.5 FROM generate_series(1, {n_rows}) g"
    )
    
    cur.execute("SELECT count(*) FROM test_seq_prefetch")
    total_count = cur.fetchone()[0]
    assert total_count == n_rows, f"Total count mismatch: {total_count} vs {n_rows}"
    
    query = "SELECT sum(id), sum(value), count(DISTINCT grp) FROM test_seq_prefetch"
    plan = _explain_plan(cur, query)
    assert "Seq Scan" in plan, f"Expected seq scan plan, got:\n{plan}"

    cur.execute(query)
    result = cur.fetchone()
    sum_id = result[0]
    sum_value = result[1]
    distinct_grps = result[2]
    
    expected_sum_id = n_rows * (n_rows + 1) // 2
    assert sum_id == expected_sum_id, f"Sum id mismatch: {sum_id} vs {expected_sum_id}"
    assert distinct_grps == 100, f"Distinct groups mismatch: {distinct_grps} vs 100"
    
    conn, cur = _restart_endpoint(env, endpoint, conn)
    
    cur.execute("SELECT count(*) FROM test_seq_prefetch")
    total_count2 = cur.fetchone()[0]
    assert total_count2 == n_rows, f"Total count after discard mismatch: {total_count2} vs {n_rows}"
    
    cur.execute(query)
    result2 = cur.fetchone()
    
    assert result2[0] == sum_id, f"Sum id mismatch after discard: {result2[0]} vs {sum_id}"
    assert result2[1] == sum_value, f"Sum value mismatch after discard: {result2[1]} vs {sum_value}"
    assert result2[2] == distinct_grps, f"Distinct groups mismatch after discard: {result2[2]} vs {distinct_grps}"


@pytest.mark.skipif(not USE_LFC, reason="LFC is disabled, skipping")
def test_prefetch_with_limit(neon_simple_env: NeonEnv):
    """
    验证 LIMIT 查询的预取正确性
    
    步骤：
    1. 创建大表
    2. 执行带 LIMIT 的查询
    3. 验证返回的数据正确且数量符合 LIMIT
    """
    env = neon_simple_env
    endpoint = env.endpoints.create_start(
        "main",
        config_lines=[
            "neon.max_file_cache_size=64MB",
            "neon.file_cache_size_limit=64MB",
            "shared_buffers=1MB",
        ],
    )
    conn = endpoint.connect()
    cur = conn.cursor()
    
    cur.execute("""
        CREATE TABLE test_limit_prefetch (
            id int4 PRIMARY KEY,
            category int4,
            value int,
            data text default repeat('a', 100)
        )
    """)
    cur.execute("CREATE INDEX idx_category_value ON test_limit_prefetch(category, value)")
    
    n_rows = 10000
    cur.execute(
        f"INSERT INTO test_limit_prefetch (id, category, value) "
        f"SELECT g, g % 10, g FROM generate_series(1, {n_rows}) g"
    )
    cur.execute("ANALYZE test_limit_prefetch")
    
    limit = 100
    query = f"SELECT id, category, value FROM test_limit_prefetch ORDER BY id LIMIT {limit}"
    cur.execute(query)
    rows = cur.fetchall()
    
    assert len(rows) == limit, f"Row count mismatch: {len(rows)} vs {limit}"
    
    for i, row in enumerate(rows):
        expected_id = i + 1
        assert row[0] == expected_id, f"ID mismatch at position {i}: {row[0]} vs {expected_id}"
        assert row[1] == expected_id % 10, f"Category mismatch at position {i}"
        assert row[2] == expected_id, f"Value mismatch at position {i}"
    
    conn, cur = _restart_endpoint(env, endpoint, conn)
    
    cur.execute(query)
    rows2 = cur.fetchall()
    
    assert len(rows2) == limit, f"Row count after discard mismatch: {len(rows2)} vs {limit}"
    
    for i, row in enumerate(rows2):
        expected_id = i + 1
        assert row[0] == expected_id, f"ID mismatch after discard at position {i}"
        assert row[1] == expected_id % 10, f"Category mismatch after discard at position {i}"
        assert row[2] == expected_id, f"Value mismatch after discard at position {i}"


@pytest.mark.skipif(not USE_LFC, reason="LFC is disabled, skipping")
def test_prefetch_with_offset_limit(neon_simple_env: NeonEnv):
    """
    验证 OFFSET + LIMIT 查询的预取正确性
    """
    env = neon_simple_env
    endpoint = env.endpoints.create_start(
        "main",
        config_lines=[
            "neon.max_file_cache_size=64MB",
            "neon.file_cache_size_limit=64MB",
            "shared_buffers=1MB",
        ],
    )
    conn = endpoint.connect()
    cur = conn.cursor()
    
    cur.execute("""
        CREATE TABLE test_offset_limit (
            id int4 PRIMARY KEY,
            value text default repeat('b', 50)
        )
    """)
    
    n_rows = 5000
    cur.execute(
        f"INSERT INTO test_offset_limit (id) SELECT g FROM generate_series(1, {n_rows}) g"
    )
    
    offset = 1000
    limit = 500
    query = f"SELECT id FROM test_offset_limit ORDER BY id OFFSET {offset} LIMIT {limit}"
    cur.execute(query)
    rows = cur.fetchall()
    
    assert len(rows) == limit
    for i, row in enumerate(rows):
        expected_id = offset + i + 1
        assert row[0] == expected_id, f"ID mismatch at position {i}: {row[0]} vs {expected_id}"
    
    conn, cur = _restart_endpoint(env, endpoint, conn)
    
    cur.execute(query)
    rows2 = cur.fetchall()
    
    assert len(rows2) == limit
    for i, row in enumerate(rows2):
        expected_id = offset + i + 1
        assert row[0] == expected_id


@pytest.mark.skipif(not USE_LFC, reason="LFC is disabled, skipping")
def test_prefetch_join_correctness(neon_simple_env: NeonEnv):
    """
    验证 JOIN 查询的预取正确性
    """
    env = neon_simple_env
    endpoint = env.endpoints.create_start(
        "main",
        config_lines=[
            "neon.max_file_cache_size=64MB",
            "neon.file_cache_size_limit=64MB",
            "shared_buffers=1MB",
        ],
    )
    conn = endpoint.connect()
    cur = conn.cursor()
    
    cur.execute("""
        CREATE TABLE test_join_a (
            id int4 PRIMARY KEY,
            category int4,
            data text default repeat('c', 100)
        )
    """)
    cur.execute("""
        CREATE TABLE test_join_b (
            id int4 PRIMARY KEY,
            a_id int4,
            value numeric,
            data text default repeat('d', 100)
        )
    """)
    cur.execute("CREATE INDEX idx_a_id ON test_join_b(a_id)")
    
    n_rows = 5000
    cur.execute(
        f"INSERT INTO test_join_a (id, category) "
        f"SELECT g, g % 10 FROM generate_series(1, {n_rows}) g"
    )
    cur.execute(
        f"INSERT INTO test_join_b (id, a_id, value) "
        f"SELECT g, (g - 1) % {n_rows} + 1, g * 1.5 FROM generate_series(1, {n_rows * 2}) g"
    )
    cur.execute("ANALYZE test_join_a")
    cur.execute("ANALYZE test_join_b")
    
    query = """
        SELECT a.id, a.category, sum(b.value) as total_value
        FROM test_join_a a
        JOIN test_join_b b ON a.id = b.a_id
        GROUP BY a.id, a.category
        ORDER BY a.id
    """
    cur.execute(query)
    results = cur.fetchall()
    
    assert len(results) == n_rows
    
    conn, cur = _restart_endpoint(env, endpoint, conn)
    
    cur.execute(query)
    results2 = cur.fetchall()
    
    assert len(results2) == len(results)
    for i, (r1, r2) in enumerate(zip(results, results2)):
        assert r1[0] == r2[0], f"ID mismatch at position {i}"
        assert r1[1] == r2[1], f"Category mismatch at position {i}"
        assert r1[2] == r2[2], f"Total value mismatch at position {i}"
