"""
事务与 LFC 交互正确性测试
验证事务操作下 LFC 的数据正确性
"""
from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from fixtures.utils import USE_LFC

if TYPE_CHECKING:
    from fixtures.neon_fixtures import NeonEnv


def _reconnect(endpoint, conn):
    conn.close()
    conn = endpoint.connect()
    return conn, conn.cursor()


@pytest.mark.skipif(not USE_LFC, reason="LFC is disabled, skipping")
def test_lfc_with_rollback(neon_simple_env: NeonEnv):
    """
    验证事务回滚后 LFC 的数据正确性
    
    步骤：
    1. 开始事务
    2. 插入/更新数据
    3. 回滚事务
    4. 验证读取的数据是回滚前的状态
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
    
    cur.execute("CREATE TABLE test_rollback (id int4 PRIMARY KEY, value int)")
    n_rows = 1000
    cur.execute(
        f"INSERT INTO test_rollback (id, value) SELECT g, g * 10 FROM generate_series(1, {n_rows}) g"
    )
    conn.commit()
    
    cur.execute("SELECT sum(value) FROM test_rollback")
    initial_sum = cur.fetchone()[0]
    expected_initial = sum(i * 10 for i in range(1, n_rows + 1))
    assert initial_sum == expected_initial
    
    conn.set_session(autocommit=False)
    
    cur.execute("UPDATE test_rollback SET value = value * 2")
    cur.execute(f"INSERT INTO test_rollback (id, value) VALUES ({n_rows + 1}, 99999)")
    
    cur.execute("SELECT sum(value) FROM test_rollback")
    modified_sum = cur.fetchone()[0]
    assert modified_sum != initial_sum
    
    conn.rollback()
    
    conn.set_session(autocommit=True)
    
    cur.execute("SELECT sum(value) FROM test_rollback")
    after_rollback_sum = cur.fetchone()[0]
    assert after_rollback_sum == initial_sum, f"Sum after rollback mismatch: {after_rollback_sum} vs {initial_sum}"
    
    cur.execute(f"SELECT count(*) FROM test_rollback WHERE id = {n_rows + 1}")
    assert cur.fetchone()[0] == 0, "Inserted row should not exist after rollback"
    
    conn, cur = _reconnect(endpoint, conn)
    
    cur.execute("SELECT sum(value) FROM test_rollback")
    final_sum = cur.fetchone()[0]
    assert final_sum == initial_sum, f"Final sum mismatch: {final_sum} vs {initial_sum}"


@pytest.mark.skipif(not USE_LFC, reason="LFC is disabled, skipping")
def test_lfc_with_savepoint(neon_simple_env: NeonEnv):
    """
    验证保存点场景下 LFC 的正确性
    
    步骤：
    1. 创建保存点
    2. 执行操作
    3. 回滚到保存点
    4. 验证数据状态
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
    
    cur.execute("CREATE TABLE test_savepoint (id int4 PRIMARY KEY, value int)")
    n_rows = 500
    cur.execute(
        f"INSERT INTO test_savepoint (id, value) SELECT g, g FROM generate_series(1, {n_rows}) g"
    )
    conn.commit()
    
    cur.execute("SELECT sum(value) FROM test_savepoint")
    initial_sum = cur.fetchone()[0]
    
    conn.set_session(autocommit=False)
    
    cur.execute("UPDATE test_savepoint SET value = value * 10 WHERE id <= 100")
    cur.execute("SAVEPOINT sp1")
    
    cur.execute("SELECT sum(value) FROM test_savepoint WHERE id <= 100")
    sum_after_first_update = cur.fetchone()[0]
    
    cur.execute("UPDATE test_savepoint SET value = value * 2 WHERE id <= 100")
    cur.execute("SAVEPOINT sp2")
    
    cur.execute("INSERT INTO test_savepoint (id, value) VALUES (999, 99999)")
    
    cur.execute("SELECT sum(value) FROM test_savepoint")
    sum_before_rollback = cur.fetchone()[0]
    
    cur.execute("ROLLBACK TO SAVEPOINT sp2")
    
    cur.execute("SELECT count(*) FROM test_savepoint WHERE id = 999")
    assert cur.fetchone()[0] == 0, "Row inserted after sp2 should not exist"
    
    cur.execute("SELECT sum(value) FROM test_savepoint WHERE id <= 100")
    sum_after_sp2_rollback = cur.fetchone()[0]
    assert sum_after_sp2_rollback == sum_after_first_update * 2
    
    cur.execute("ROLLBACK TO SAVEPOINT sp1")
    
    cur.execute("SELECT sum(value) FROM test_savepoint WHERE id <= 100")
    sum_after_sp1_rollback = cur.fetchone()[0]
    assert sum_after_sp1_rollback == sum_after_first_update
    
    conn.commit()
    
    conn.set_session(autocommit=True)
    
    conn, cur = _reconnect(endpoint, conn)
    
    cur.execute("SELECT sum(value) FROM test_savepoint WHERE id <= 100")
    final_sum = cur.fetchone()[0]
    assert final_sum == sum_after_first_update


@pytest.mark.skipif(not USE_LFC, reason="LFC is disabled, skipping")
def test_lfc_with_isolation_levels(neon_simple_env: NeonEnv):
    """
    验证不同隔离级别下 LFC 的正确性
    
    步骤：
    1. 设置不同的事务隔离级别
    2. 执行并发事务
    3. 验证每个隔离级别的语义正确
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
    
    main_conn = endpoint.connect()
    main_cur = main_conn.cursor()
    
    main_cur.execute("CREATE TABLE test_isolation (id int4 PRIMARY KEY, value int)")
    n_rows = 1000
    main_cur.execute(
        f"INSERT INTO test_isolation (id, value) SELECT g, g FROM generate_series(1, {n_rows}) g"
    )
    main_conn.commit()
    
    main_cur.execute("SELECT sum(value) FROM test_isolation")
    initial_sum = main_cur.fetchone()[0]
    
    conn1 = endpoint.connect()
    cur1 = conn1.cursor()
    conn1.set_session(isolation_level="REPEATABLE READ", autocommit=False)
    
    cur1.execute("SELECT sum(value) FROM test_isolation")
    sum_conn1_before = cur1.fetchone()[0]
    
    conn2 = endpoint.connect()
    cur2 = conn2.cursor()
    conn2.set_session(autocommit=False)
    
    cur2.execute("UPDATE test_isolation SET value = value + 1000 WHERE id <= 100")
    conn2.commit()
    
    cur1.execute("SELECT sum(value) FROM test_isolation")
    sum_conn1_after = cur1.fetchone()[0]
    
    assert sum_conn1_after == sum_conn1_before, (
        f"REPEATABLE READ should see same data: {sum_conn1_after} vs {sum_conn1_before}"
    )
    
    conn1.commit()
    
    cur1.execute("SELECT sum(value) FROM test_isolation")
    sum_conn1_new = cur1.fetchone()[0]
    assert sum_conn1_new != sum_conn1_before, "After commit, should see new data"
    
    conn1.close()
    conn2.close()
    
    main_conn, main_cur = _reconnect(endpoint, main_conn)
    
    main_cur.execute("SELECT sum(value) FROM test_isolation")
    final_sum = main_cur.fetchone()[0]
    expected_final = initial_sum + 100 * 1000
    assert final_sum == expected_final, f"Final sum mismatch: {final_sum} vs {expected_final}"


@pytest.mark.skipif(not USE_LFC, reason="LFC is disabled, skipping")
def test_lfc_with_read_committed(neon_simple_env: NeonEnv):
    """
    验证 READ COMMITTED 隔离级别下 LFC 的正确性
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
    
    main_conn = endpoint.connect()
    main_cur = main_conn.cursor()
    
    main_cur.execute("CREATE TABLE test_read_committed (id int4 PRIMARY KEY, value int)")
    main_cur.execute("INSERT INTO test_read_committed VALUES (1, 100), (2, 200)")
    main_conn.commit()
    
    conn1 = endpoint.connect()
    cur1 = conn1.cursor()
    conn1.set_session(isolation_level="READ COMMITTED", autocommit=False)
    
    cur1.execute("SELECT value FROM test_read_committed WHERE id = 1")
    value_before = cur1.fetchone()[0]
    assert value_before == 100
    
    conn2 = endpoint.connect()
    cur2 = conn2.cursor()
    cur2.execute("UPDATE test_read_committed SET value = 999 WHERE id = 1")
    conn2.commit()
    conn2.close()
    
    cur1.execute("SELECT value FROM test_read_committed WHERE id = 1")
    value_after = cur1.fetchone()[0]
    assert value_after == 999, f"READ COMMITTED should see committed changes: {value_after}"
    
    conn1.commit()
    conn1.close()
    
    main_conn, main_cur = _reconnect(endpoint, main_conn)
    
    main_cur.execute("SELECT value FROM test_read_committed WHERE id = 1")
    final_value = main_cur.fetchone()[0]
    assert final_value == 999


@pytest.mark.skipif(not USE_LFC, reason="LFC is disabled, skipping")
def test_lfc_with_serializable(neon_simple_env: NeonEnv):
    """
    验证 SERIALIZABLE 隔离级别下 LFC 的正确性。

    构造确定性场景：SERIALIZABLE 事务 T1 读取一行后，另一 session 修改并提交
    该行，T1 再尝试更新同一行时必须失败（concurrent update 冲突）。
    验证回滚后通过 LFC 读取的数据与已提交修改一致。
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

    main_conn = endpoint.connect()
    main_conn.autocommit = True
    main_cur = main_conn.cursor()

    main_cur.execute("CREATE TABLE test_serializable (id int4 PRIMARY KEY, value int)")
    main_cur.execute("INSERT INTO test_serializable VALUES (1, 100), (2, 200)")

    conn1 = endpoint.connect()
    cur1 = conn1.cursor()
    cur1.execute("BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE")
    cur1.execute("SELECT value FROM test_serializable WHERE id = 1")
    assert cur1.fetchone()[0] == 100

    modifier = endpoint.connect()
    modifier.autocommit = True
    mod_cur = modifier.cursor()
    mod_cur.execute("UPDATE test_serializable SET value = 999 WHERE id = 1")
    modifier.close()

    serialization_error = False
    try:
        cur1.execute("UPDATE test_serializable SET value = value + 50 WHERE id = 1")
        cur1.execute("COMMIT")
    except Exception as e:
        err_msg = str(e).lower()
        if "serialize" in err_msg or "concurrent update" in err_msg or "conflict" in err_msg:
            serialization_error = True
            try:
                cur1.execute("ROLLBACK")
            except Exception:
                pass
        else:
            pytest.fail(f"Unexpected error: {e}")

    conn1.close()

    assert serialization_error, (
        "SERIALIZABLE transaction that reads a row modified concurrently "
        "should fail on write conflict"
    )

    main_cur.execute("SELECT value FROM test_serializable WHERE id = 1")
    final_value = main_cur.fetchone()[0]
    assert final_value == 999, (
        f"After T1 rollback, only the modifier's update should persist: "
        f"expected 999, got {final_value}"
    )

    main_cur.execute("SELECT value FROM test_serializable WHERE id = 2")
    assert main_cur.fetchone()[0] == 200, "Unmodified row should remain unchanged"


@pytest.mark.skipif(not USE_LFC, reason="LFC is disabled, skipping")
def test_lfc_with_transaction_after_discard(neon_simple_env: NeonEnv):
    """
    验证会话重连后事务的正确性
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
    
    cur.execute("CREATE TABLE test_txn_discard (id int4 PRIMARY KEY, value int)")
    cur.execute("INSERT INTO test_txn_discard VALUES (1, 100), (2, 200), (3, 300)")
    conn.commit()
    
    cur.execute("SELECT sum(value) FROM test_txn_discard")
    initial_sum = cur.fetchone()[0]
    assert initial_sum == 600
    
    conn.set_session(autocommit=False)
    cur.execute("UPDATE test_txn_discard SET value = value * 2")
    conn.commit()
    
    cur.execute("SELECT sum(value) FROM test_txn_discard")
    updated_sum = cur.fetchone()[0]
    assert updated_sum == 1200
    
    conn, cur = _reconnect(endpoint, conn)
    
    conn.set_session(autocommit=False)
    cur.execute("UPDATE test_txn_discard SET value = value + 1000")
    conn.commit()
    
    cur.execute("SELECT sum(value) FROM test_txn_discard")
    final_sum = cur.fetchone()[0]
    assert final_sum == 1200 + 3000, f"Final sum mismatch: {final_sum}"


@pytest.mark.skipif(not USE_LFC, reason="LFC is disabled, skipping")
def test_lfc_with_multiple_transactions(neon_simple_env: NeonEnv):
    """
    验证多个连续事务的数据正确性
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
    
    cur.execute("CREATE TABLE test_multi_txn (id int4 PRIMARY KEY, value int, version int)")
    cur.execute("INSERT INTO test_multi_txn VALUES (1, 0, 0)")
    conn.commit()
    
    n_transactions = 10
    for i in range(n_transactions):
        conn.set_session(autocommit=False)
        cur.execute("UPDATE test_multi_txn SET value = value + 1, version = version + 1 WHERE id = 1")
        conn.commit()
    
    cur.execute("SELECT value, version FROM test_multi_txn WHERE id = 1")
    result = cur.fetchone()
    assert result[0] == n_transactions, f"Value mismatch: {result[0]} vs {n_transactions}"
    assert result[1] == n_transactions, f"Version mismatch: {result[1]} vs {n_transactions}"
    
    conn, cur = _reconnect(endpoint, conn)
    
    cur.execute("SELECT value, version FROM test_multi_txn WHERE id = 1")
    result2 = cur.fetchone()
    assert result2[0] == n_transactions
    assert result2[1] == n_transactions
