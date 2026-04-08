"""
边界条件数据正确性测试
验证极端场景下 LFC 的数据正确性
"""
from __future__ import annotations

import random
import time
from typing import TYPE_CHECKING

import pytest
from fixtures.neon_fixtures import wait_for_last_flush_lsn
from fixtures.utils import USE_LFC

if TYPE_CHECKING:
    from fixtures.neon_fixtures import NeonEnv


def _reconnect(endpoint, conn):
    conn.close()
    conn = endpoint.connect()
    return conn, conn.cursor()


def _build_payload(row_id: int, width: int = 1024) -> str:
    seed = (row_id * 1103515245 + 12345) & 0xFFFFFFFF
    chunks: list[str] = []
    while len(chunks) * 8 < width:
        seed = (seed * 1664525 + 1013904223) & 0xFFFFFFFF
        chunks.append(f"{seed:08x}")
    return "".join(chunks)[:width]


def _actual_relation_size_bytes(cur, relation_name: str) -> int:
    for query in (
        f"SELECT pg_total_relation_size('{relation_name}'::regclass)",
        f"SELECT pg_relation_size('{relation_name}'::regclass)",
    ):
        try:
            cur.execute(query)
            row = cur.fetchone()
            size_bytes = int(row[0]) if row and row[0] is not None else 0
            if size_bytes > 0:
                return size_bytes
        except Exception:
            cur.connection.rollback()

    cur.execute(f"SELECT COALESCE(sum(pg_column_size(t)), 0) FROM {relation_name} t")
    row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else 0


@pytest.mark.skipif(not USE_LFC, reason="LFC is disabled, skipping")
def test_lfc_full_capacity_correctness(neon_simple_env: NeonEnv):
    """
    验证 LFC 满容量时的数据正确性
    
    步骤：
    1. 设置较小的 LFC 容量
    2. 插入超过容量的数据
    3. 验证所有查询结果正确
    """
    env = neon_simple_env
    cache_size_mb = 4
    endpoint = env.endpoints.create_start(
        "main",
        config_lines=[
            f"neon.max_file_cache_size={cache_size_mb}MB",
            f"neon.file_cache_size_limit={cache_size_mb}MB",
            "shared_buffers=1MB",
        ],
    )
    conn = endpoint.connect()
    cur = conn.cursor()
    
    cur.execute("""
        CREATE TABLE test_full_capacity (
            id int4 PRIMARY KEY,
            value int,
            data text
        )
    """)

    target_table_bytes = cache_size_mb * 1024 * 1024
    n_rows = 0
    actual_size_bytes = 0
    batch_size = 400 if env.pg_version.is_opengauss else 600
    max_rows = 8000

    while actual_size_bytes < target_table_bytes and n_rows < max_rows:
        start_id = n_rows + 1
        end_id = min(n_rows + batch_size, max_rows)

        cur.executemany(
            "INSERT INTO test_full_capacity (id, value, data) VALUES (%s, %s, %s)",
            [(i, i * 10, _build_payload(i)) for i in range(start_id, end_id + 1)],
        )
        n_rows = end_id
        actual_size_bytes = _actual_relation_size_bytes(cur, "test_full_capacity")

    assert actual_size_bytes >= target_table_bytes, (
        f"Table size {actual_size_bytes} bytes did not exceed target {target_table_bytes} bytes"
    )
    wait_for_last_flush_lsn(env, endpoint, env.initial_tenant, env.initial_timeline)
    
    expected_sum = n_rows * (n_rows + 1) // 2
    expected_value_sum = expected_sum * 10
    
    cur.execute("SELECT sum(id), sum(value), count(*) FROM test_full_capacity")
    result = cur.fetchone()
    assert result[0] == expected_sum, f"Sum id mismatch: {result[0]} vs {expected_sum}"
    assert result[1] == expected_value_sum, f"Sum value mismatch: {result[1]} vs {expected_value_sum}"
    assert result[2] == n_rows, f"Count mismatch: {result[2]} vs {n_rows}"
    
    for _ in range(3):
        conn, cur = _reconnect(endpoint, conn)
        
        cur.execute("SELECT sum(id), sum(value), count(*) FROM test_full_capacity")
        result = cur.fetchone()
        assert result[0] == expected_sum, f"Sum id mismatch after discard: {result[0]} vs {expected_sum}"
        assert result[1] == expected_value_sum, f"Sum value mismatch after discard: {result[1]} vs {expected_value_sum}"
        assert result[2] == n_rows, f"Count mismatch after discard: {result[2]} vs {n_rows}"
    
    cur.execute("SELECT id, value FROM test_full_capacity WHERE id % 1000 = 1 ORDER BY id")
    sample_rows = cur.fetchall()
    for row in sample_rows:
        expected_value = row[0] * 10
        assert row[1] == expected_value, f"Value mismatch for id {row[0]}: {row[1]} vs {expected_value}"


@pytest.mark.skipif(not USE_LFC, reason="LFC is disabled, skipping")
def test_lfc_resize_during_query(neon_simple_env: NeonEnv):
    """
    验证 LFC resize 与活跃查询并发时的数据正确性。

    后台线程反复执行全表扫描，主线程同时快速调整 LFC 大小，
    验证并发竞态窗口下查询结果仍然正确。
    """
    import threading
    import queue as queue_mod

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
        CREATE TABLE test_resize_during_query (
            id int4,
            grp int,
            value numeric,
            data text default repeat('y', 200)
        )
    """)

    n_rows = 20000
    cur.execute(
        f"INSERT INTO test_resize_during_query (id, grp, value) "
        f"SELECT g, g % 100, g * 1.5 FROM generate_series(1, {n_rows}) g"
    )

    expected_sum = n_rows * (n_rows + 1) // 2
    expected_value_sum = expected_sum * 3 // 2

    stop_event = threading.Event()
    errors: queue_mod.Queue = queue_mod.Queue()

    def reader_loop():
        rconn = endpoint.connect()
        rcur = rconn.cursor()
        try:
            while not stop_event.is_set():
                rcur.execute(
                    "SELECT sum(id), sum(value), count(*) FROM test_resize_during_query"
                )
                r = rcur.fetchone()
                if r[0] != expected_sum or r[1] != expected_value_sum or r[2] != n_rows:
                    errors.put(
                        f"Mismatch: sum_id={r[0]}, sum_value={r[1]}, count={r[2]}"
                    )
                    return
        except Exception as e:
            errors.put(str(e))
        finally:
            rcur.close()
            rconn.close()

    readers = [threading.Thread(target=reader_loop) for _ in range(3)]
    for t in readers:
        t.start()

    resize_values = [32, 16, 48, 8, 64, 4, 32, 16, 64]
    for size_mb in resize_values:
        cur.execute(f"ALTER SYSTEM SET neon.file_cache_size_limit='{size_mb}MB'")
        cur.execute("SELECT pg_reload_conf()")
        time.sleep(0.05)

    cur.execute("ALTER SYSTEM SET neon.file_cache_size_limit='64MB'")
    cur.execute("SELECT pg_reload_conf()")

    stop_event.set()
    for t in readers:
        t.join()

    thread_errors = []
    while not errors.empty():
        thread_errors.append(errors.get())
    if thread_errors:
        pytest.fail(f"Reader errors during LFC resize: {thread_errors}")

    cur.execute("SELECT sum(id), sum(value), count(*) FROM test_resize_during_query")
    result_final = cur.fetchone()
    assert result_final[0] == expected_sum
    assert result_final[1] == expected_value_sum
    assert result_final[2] == n_rows


@pytest.mark.skipif(not USE_LFC, reason="LFC is disabled, skipping")
def test_prefetch_buffer_resize_during_query(neon_simple_env: NeonEnv):
    """
    验证查询过程中调整预取缓冲区的正确性
    
    步骤：
    1. 启动带预取的查询
    2. 在查询过程中调整 readahead_buffer_size
    3. 验证查询结果正确
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
        CREATE TABLE test_buffer_resize (
            id int4 PRIMARY KEY,
            category int4,
            value int,
            data text default repeat('z', 100)
        )
    """)
    cur.execute("CREATE INDEX idx_category ON test_buffer_resize(category)")
    
    n_rows = 10000
    cur.execute(
        f"INSERT INTO test_buffer_resize (id, category, value) "
        f"SELECT g, g % 50, g * 10 FROM generate_series(1, {n_rows}) g"
    )
    cur.execute("ANALYZE test_buffer_resize")
    
    expected_sum = n_rows * (n_rows + 1) // 2
    expected_value_sum = expected_sum * 10
    
    buffer_sizes = [32, 64, 16, 128, 48]
    for buf_size in buffer_sizes:
        cur.execute(f"SET neon.readahead_buffer_size={buf_size}")
        
        cur.execute("SELECT sum(id), sum(value), count(*) FROM test_buffer_resize")
        result = cur.fetchone()
        
        assert result[0] == expected_sum, f"Sum id mismatch with buffer {buf_size}: {result[0]} vs {expected_sum}"
        assert result[1] == expected_value_sum, f"Sum value mismatch with buffer {buf_size}"
        assert result[2] == n_rows, f"Count mismatch with buffer {buf_size}"
    
    conn, cur = _reconnect(endpoint, conn)
    
    cur.execute("SELECT sum(id), sum(value), count(*) FROM test_buffer_resize")
    result_final = cur.fetchone()
    
    assert result_final[0] == expected_sum
    assert result_final[1] == expected_value_sum
    assert result_final[2] == n_rows


@pytest.mark.skipif(not USE_LFC, reason="LFC is disabled, skipping")
def test_lfc_with_toast_data(neon_simple_env: NeonEnv):
    """
    验证 TOAST 数据在 LFC 中的正确性
    
    步骤：
    1. 创建包含大字段的表
    2. 插入 TOAST 数据
    3. 读取并验证数据完整性
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
        CREATE TABLE test_toast (
            id int4 PRIMARY KEY,
            large_text text,
            large_bytea bytea,
            value int
        )
    """)
    
    n_rows = 100
    toast_size = 10000
    
    for i in range(1, n_rows + 1):
        large_text = 'x' * toast_size + str(i)
        large_bytea = b'y' * toast_size + str(i).encode()
        cur.execute(
            "INSERT INTO test_toast (id, large_text, large_bytea, value) VALUES (%s, %s, %s, %s)",
            (i, large_text, large_bytea, i * 100)
        )
    
    cur.execute("SELECT count(*) FROM test_toast")
    count = cur.fetchone()[0]
    assert count == n_rows
    
    for i in range(1, n_rows + 1):
        expected_text = 'x' * toast_size + str(i)
        expected_bytea = b'y' * toast_size + str(i).encode()
        
        cur.execute("SELECT large_text, large_bytea, value FROM test_toast WHERE id = %s", (i,))
        row = cur.fetchone()
        actual_bytea = bytes(row[1]) if isinstance(row[1], memoryview) else row[1]
        
        assert row[0] == expected_text, f"Text mismatch for id {i}"
        assert actual_bytea == expected_bytea, f"Bytea mismatch for id {i}"
        assert row[2] == i * 100, f"Value mismatch for id {i}"
    
    conn, cur = _reconnect(endpoint, conn)
    
    random_ids = random.sample(range(1, n_rows + 1), 20)
    for i in random_ids:
        expected_text = 'x' * toast_size + str(i)
        expected_bytea = b'y' * toast_size + str(i).encode()
        
        cur.execute("SELECT large_text, large_bytea, value FROM test_toast WHERE id = %s", (i,))
        row = cur.fetchone()
        actual_bytea = bytes(row[1]) if isinstance(row[1], memoryview) else row[1]
        
        assert row[0] == expected_text, f"Text mismatch after discard for id {i}"
        assert actual_bytea == expected_bytea, f"Bytea mismatch after discard for id {i}"
        assert row[2] == i * 100, f"Value mismatch after discard for id {i}"


@pytest.mark.skipif(not USE_LFC, reason="LFC is disabled, skipping")
def test_lfc_with_null_values(neon_simple_env: NeonEnv):
    """
    验证 NULL 值在 LFC 中的正确性
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
        CREATE TABLE test_nulls (
            id int4 PRIMARY KEY,
            nullable_int int,
            nullable_text text,
            nullable_numeric numeric,
            non_null_value int not null
        )
    """)
    
    n_rows = 1000
    cur.execute(
        f"INSERT INTO test_nulls (id, nullable_int, nullable_text, nullable_numeric, non_null_value) "
        f"SELECT g, "
        f"CASE WHEN g % 3 = 0 THEN NULL ELSE g END, "
        f"CASE WHEN g % 5 = 0 THEN NULL ELSE 'text_' || g END, "
        f"CASE WHEN g % 7 = 0 THEN NULL ELSE g * 1.5 END, "
        f"g * 10 "
        f"FROM generate_series(1, {n_rows}) g"
    )
    
    cur.execute("SELECT count(*), count(nullable_int), count(nullable_text), count(nullable_numeric) FROM test_nulls")
    result = cur.fetchone()
    assert result[0] == n_rows
    assert result[1] == n_rows - n_rows // 3
    assert result[2] == n_rows - n_rows // 5
    assert result[3] == n_rows - n_rows // 7
    
    conn, cur = _reconnect(endpoint, conn)
    
    cur.execute("SELECT count(*), count(nullable_int), count(nullable_text), count(nullable_numeric) FROM test_nulls")
    result2 = cur.fetchone()
    assert result2[0] == result[0]
    assert result2[1] == result[1]
    assert result2[2] == result[2]
    assert result2[3] == result[3]
    
    cur.execute("SELECT id, nullable_int, nullable_text, nullable_numeric, non_null_value FROM test_nulls WHERE id % 100 = 1")
    rows = cur.fetchall()
    for row in rows:
        id = row[0]
        nullable_int = row[1]
        nullable_text = row[2]
        nullable_numeric = row[3]
        non_null_value = row[4]
        
        assert non_null_value == id * 10, f"Non-null value mismatch for id {id}"
        
        if id % 3 == 0:
            assert nullable_int is None, f"nullable_int should be NULL for id {id}"
        else:
            assert nullable_int == id, f"nullable_int mismatch for id {id}"
        
        if id % 5 == 0:
            assert nullable_text is None, f"nullable_text should be NULL for id {id}"
        else:
            assert nullable_text == f'text_{id}', f"nullable_text mismatch for id {id}"


@pytest.mark.skipif(not USE_LFC, reason="LFC is disabled, skipping")
def test_lfc_with_large_rows(neon_simple_env: NeonEnv):
    """
    验证大行数据在 LFC 中的正确性
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
        CREATE TABLE test_large_rows (
            id int4 PRIMARY KEY,
            data1 text,
            data2 text,
            data3 text,
            value int
        )
    """)
    
    n_rows = 50
    data_size = 5000
    
    for i in range(1, n_rows + 1):
        data1 = 'a' * data_size
        data2 = 'b' * data_size
        data3 = 'c' * data_size
        cur.execute(
            "INSERT INTO test_large_rows (id, data1, data2, data3, value) VALUES (%s, %s, %s, %s, %s)",
            (i, data1, data2, data3, i * 1000)
        )
    
    cur.execute("SELECT sum(value) FROM test_large_rows")
    expected_sum = sum(i * 1000 for i in range(1, n_rows + 1))
    result = cur.fetchone()[0]
    assert result == expected_sum
    
    conn, cur = _reconnect(endpoint, conn)
    
    for i in [1, n_rows // 2, n_rows]:
        cur.execute("SELECT data1, data2, data3, value FROM test_large_rows WHERE id = %s", (i,))
        row = cur.fetchone()
        
        assert row[0] == 'a' * data_size, f"data1 mismatch for id {i}"
        assert row[1] == 'b' * data_size, f"data2 mismatch for id {i}"
        assert row[2] == 'c' * data_size, f"data3 mismatch for id {i}"
        assert row[3] == i * 1000, f"value mismatch for id {i}"
