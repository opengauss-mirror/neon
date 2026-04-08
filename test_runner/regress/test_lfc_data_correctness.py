"""
LFC 数据正确性测试
验证从 LFC 读取的数据与从 pageserver 读取的数据一致
"""
from __future__ import annotations

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


def _restart_and_reconnect(endpoint, conn):
    env = endpoint.env
    timeline_id = env.initial_timeline

    cur = conn.cursor()
    cur.execute("CHECKPOINT")
    cur.close()

    wait_for_last_flush_lsn(env, endpoint, endpoint.tenant_id, timeline_id)
    conn.close()
    time.sleep(1)
    endpoint.stop(
        mode="immediate",
        sks_wait_walreceiver_gone=(env.safekeepers, timeline_id),
    )
    endpoint.start()
    conn = endpoint.connect()
    return conn, conn.cursor()


@pytest.mark.skipif(not USE_LFC, reason="LFC is disabled, skipping")
def test_lfc_read_consistency(neon_simple_env: NeonEnv):
    """
    验证从 LFC 读取的数据与从 pageserver 读取的数据一致
    
    步骤：
    1. 创建表并插入数据
    2. 第一次读取（填充 LFC）
    3. 清空 shared_buffers
    4. 第二次读取（从 LFC 读取）
    5. 比较两次读取结果
    """
    env = neon_simple_env
    endpoint = env.endpoints.create_start(
        "main",
        config_lines=[
            "neon.max_file_cache_size=64MB",
            "neon.file_cache_size_limit=64MB",
            "shared_buffers=1MB",
            "autovacuum=off",
        ],
    )
    conn = endpoint.connect()
    cur = conn.cursor()
    
    cur.execute("CREATE TABLE test_data (id int4 PRIMARY KEY, value text, num int)")
    n_rows = 10000
    cur.execute(
        f"INSERT INTO test_data SELECT g, 'value_' || g, g * 100 FROM generate_series(1, {n_rows}) g"
    )
    
    cur.execute("SELECT sum(id), sum(num), count(*) FROM test_data")
    first_result = cur.fetchone()
    first_sum_id = first_result[0]
    first_sum_num = first_result[1]
    first_count = first_result[2]
    
    expected_sum_id = n_rows * (n_rows + 1) // 2
    expected_sum_num = 100 * expected_sum_id
    assert first_sum_id == expected_sum_id, f"Expected sum(id)={expected_sum_id}, got {first_sum_id}"
    assert first_sum_num == expected_sum_num, f"Expected sum(num)={expected_sum_num}, got {first_sum_num}"
    assert first_count == n_rows, f"Expected count={n_rows}, got {first_count}"
    
    conn, cur = _restart_and_reconnect(endpoint, conn)
    
    cur.execute("SELECT sum(id), sum(num), count(*) FROM test_data")
    second_result = cur.fetchone()
    second_sum_id = second_result[0]
    second_sum_num = second_result[1]
    second_count = second_result[2]
    
    assert second_sum_id == first_sum_id, f"Data inconsistency: first={first_sum_id}, second={second_sum_id}"
    assert second_sum_num == first_sum_num, f"Data inconsistency: first={first_sum_num}, second={second_sum_num}"
    assert second_count == first_count, f"Data inconsistency: first={first_count}, second={second_count}"


@pytest.mark.skipif(not USE_LFC, reason="LFC is disabled, skipping")
def test_lfc_with_updates(neon_simple_env: NeonEnv):
    """
    验证更新操作后 LFC 中数据的一致性
    
    步骤：
    1. 创建表并插入数据
    2. 读取数据填充 LFC
    3. 更新数据
    4. 再次读取验证最新数据
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
    
    cur.execute("CREATE TABLE test_updates (id int4 PRIMARY KEY, value int)")
    n_rows = 1000
    cur.execute(f"INSERT INTO test_updates SELECT g, g FROM generate_series(1, {n_rows}) g")
    
    cur.execute("SELECT sum(value) FROM test_updates")
    initial_sum = cur.fetchone()[0]
    expected_initial = n_rows * (n_rows + 1) // 2
    assert initial_sum == expected_initial, f"Initial sum mismatch: {initial_sum} vs {expected_initial}"
    
    update_factor = 10
    cur.execute(f"UPDATE test_updates SET value = value * {update_factor}")
    
    cur.execute("SELECT sum(value) FROM test_updates")
    updated_sum = cur.fetchone()[0]
    expected_updated = expected_initial * update_factor
    assert updated_sum == expected_updated, f"Updated sum mismatch: {updated_sum} vs {expected_updated}"
    
    conn, cur = _restart_and_reconnect(endpoint, conn)
    
    cur.execute("SELECT sum(value) FROM test_updates")
    final_sum = cur.fetchone()[0]
    assert final_sum == expected_updated, f"Data after DISCARD mismatch: {final_sum} vs {expected_updated}"
    
    cur.execute("SELECT count(*) FROM test_updates WHERE value = id * 10")
    correct_rows = cur.fetchone()[0]
    assert correct_rows == n_rows, f"Not all rows updated correctly: {correct_rows} vs {n_rows}"


@pytest.mark.skipif(not USE_LFC, reason="LFC is disabled, skipping")
def test_lfc_with_deletes(neon_simple_env: NeonEnv):
    """
    验证删除操作后 LFC 的行为
    
    步骤：
    1. 创建表并插入数据
    2. 读取数据填充 LFC
    3. 删除部分数据
    4. 查询验证删除生效
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
    
    cur.execute("CREATE TABLE test_deletes (id int4 PRIMARY KEY, value text)")
    n_rows = 10000
    cur.execute(
        f"INSERT INTO test_deletes SELECT g, 'value_' || g FROM generate_series(1, {n_rows}) g"
    )
    
    cur.execute("SELECT count(*) FROM test_deletes")
    initial_count = cur.fetchone()[0]
    assert initial_count == n_rows
    
    delete_threshold = n_rows // 2
    cur.execute(f"DELETE FROM test_deletes WHERE id > {delete_threshold}")
    deleted_count = cur.rowcount
    
    expected_deleted = n_rows - delete_threshold
    assert deleted_count == expected_deleted, f"Deleted count mismatch: {deleted_count} vs {expected_deleted}"
    
    conn, cur = _restart_and_reconnect(endpoint, conn)
    
    cur.execute("SELECT count(*) FROM test_deletes")
    final_count = cur.fetchone()[0]
    expected_final = delete_threshold
    assert final_count == expected_final, f"Final count mismatch: {final_count} vs {expected_final}"
    
    cur.execute(f"SELECT count(*) FROM test_deletes WHERE id > {delete_threshold}")
    remaining_high_ids = cur.fetchone()[0]
    assert remaining_high_ids == 0, f"Should have no rows with id > {delete_threshold}"
    
    cur.execute(f"SELECT count(*) FROM test_deletes WHERE id <= {delete_threshold}")
    remaining_low_ids = cur.fetchone()[0]
    assert remaining_low_ids == delete_threshold, f"Should have all rows with id <= {delete_threshold}"


@pytest.mark.skipif(not USE_LFC, reason="LFC is disabled, skipping")
def test_lfc_with_vacuum(neon_simple_env: NeonEnv):
    """
    验证 VACUUM 操作后 LFC 的行为
    
    步骤：
    1. 创建表并进行大量更新/删除
    2. 执行 VACUUM
    3. 验证后续查询结果正确
    """
    env = neon_simple_env
    endpoint = env.endpoints.create_start(
        "main",
        config_lines=[
            "neon.max_file_cache_size=64MB",
            "neon.file_cache_size_limit=64MB",
            "shared_buffers=1MB",
            "autovacuum=off",
        ],
    )
    conn = endpoint.connect()
    cur = conn.cursor()
    
    cur.execute("CREATE TABLE test_vacuum (id int4 PRIMARY KEY, value int, filler text default repeat('x', 100))")
    n_rows = 1000
    cur.execute(f"INSERT INTO test_vacuum SELECT g, g FROM generate_series(1, {n_rows}) g")
    
    cur.execute("SELECT sum(value) FROM test_vacuum")
    initial_sum = cur.fetchone()[0]
    
    cur.execute("UPDATE test_vacuum SET value = value * 2 WHERE id % 2 = 0")
    
    cur.execute("DELETE FROM test_vacuum WHERE id % 3 = 0")
    
    cur.execute("VACUUM ANALYZE test_vacuum")
    
    cur.execute("SELECT sum(value) FROM test_vacuum")
    after_vacuum_sum = cur.fetchone()[0]
    
    conn, cur = _restart_and_reconnect(endpoint, conn)
    
    cur.execute("SELECT sum(value) FROM test_vacuum")
    final_sum = cur.fetchone()[0]
    
    assert final_sum == after_vacuum_sum, f"Sum mismatch after DISCARD: {final_sum} vs {after_vacuum_sum}"
    
    cur.execute("SELECT count(*) FROM test_vacuum WHERE id % 3 = 0")
    deleted_rows = cur.fetchone()[0]
    assert deleted_rows == 0, "Rows with id % 3 = 0 should have been deleted"
    
    cur.execute("SELECT count(*) FROM test_vacuum")
    final_count = cur.fetchone()[0]
    expected_count = n_rows - n_rows // 3
    assert final_count == expected_count, f"Count mismatch: {final_count} vs {expected_count}"


@pytest.mark.skipif(not USE_LFC, reason="LFC is disabled, skipping")
def test_lfc_multiple_reads_consistency(neon_simple_env: NeonEnv):
    """
    验证多次读取数据的一致性
    
    步骤：
    1. 创建表并插入数据
    2. 多次读取同一数据
    3. 验证每次读取结果一致
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
    
    cur.execute("CREATE TABLE test_multi_read (id int4, grp int, value numeric)")
    n_rows = 10000
    cur.execute(
        f"INSERT INTO test_multi_read SELECT g, g % 100, g * 1.5 FROM generate_series(1, {n_rows}) g"
    )
    
    results = []
    for i in range(5):
        cur.execute("SELECT grp, sum(value) as total FROM test_multi_read GROUP BY grp ORDER BY grp")
        group_sums = cur.fetchall()
        results.append(group_sums)
        
        if i < 4:
            conn, cur = _reconnect(endpoint, conn)
    
    for i in range(1, len(results)):
        assert results[i] == results[0], f"Result {i} differs from first result"


@pytest.mark.skipif(not USE_LFC, reason="LFC is disabled, skipping")
def test_lfc_with_indexes(neon_simple_env: NeonEnv):
    """
    验证带索引的表在 LFC 中的数据正确性
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
        CREATE TABLE test_indexes (
            id int4 PRIMARY KEY,
            category int4,
            value text,
            created_at timestamp default now()
        )
    """)
    cur.execute("CREATE INDEX idx_category ON test_indexes(category)")
    cur.execute("CREATE INDEX idx_value ON test_indexes(value)")
    
    n_rows = 5000
    cur.execute(
        f"INSERT INTO test_indexes (id, category, value) "
        f"SELECT g, g % 10, 'val_' || g FROM generate_series(1, {n_rows}) g"
    )
    
    cur.execute("SELECT count(*) FROM test_indexes WHERE category = 5")
    expected_count = n_rows // 10
    first_count = cur.fetchone()[0]
    assert first_count == expected_count
    
    cur.execute("SELECT sum(id) FROM test_indexes WHERE category = 5")
    first_sum = cur.fetchone()[0]
    
    conn, cur = _restart_and_reconnect(endpoint, conn)
    
    cur.execute("SELECT count(*) FROM test_indexes WHERE category = 5")
    second_count = cur.fetchone()[0]
    assert second_count == first_count
    
    cur.execute("SELECT sum(id) FROM test_indexes WHERE category = 5")
    second_sum = cur.fetchone()[0]
    assert second_sum == first_sum
