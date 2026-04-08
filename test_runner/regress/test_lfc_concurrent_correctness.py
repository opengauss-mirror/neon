"""
并发场景数据正确性测试
验证并发操作下 LFC 的数据正确性
"""
from __future__ import annotations

import queue
import random
import threading
import time
from typing import TYPE_CHECKING

import pytest
from fixtures.utils import USE_LFC

if TYPE_CHECKING:
    from fixtures.neon_fixtures import NeonEnv


@pytest.mark.skipif(not USE_LFC, reason="LFC is disabled, skipping")
def test_concurrent_reads_with_lfc(neon_simple_env: NeonEnv):
    """
    验证并发读取时 LFC 的数据正确性
    
    步骤：
    1. 创建表并插入数据
    2. 启动多个并发读取线程
    3. 验证每个线程读取的数据一致
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
    
    main_cur.execute("CREATE TABLE test_concurrent_read (id int4 PRIMARY KEY, value int, data text default repeat('x', 100))")
    n_rows = 10000
    main_cur.execute(
        f"INSERT INTO test_concurrent_read (id, value) "
        f"SELECT g, g * 10 FROM generate_series(1, {n_rows}) g"
    )
    
    expected_sum = n_rows * (n_rows + 1) // 2
    expected_value_sum = expected_sum * 10
    expected_sample_count = (n_rows - 1) // 1000 + 1
    
    results_queue: queue.Queue = queue.Queue()
    errors_queue: queue.Queue = queue.Queue()
    n_threads = 5
    n_iterations = 10
    
    def reader_thread(thread_id: int):
        conn = endpoint.connect()
        cur = conn.cursor()
        local_results = []
        try:
            for _ in range(n_iterations):
                cur.execute("SELECT sum(id), sum(value), count(*) FROM test_concurrent_read")
                result = cur.fetchone()
                local_results.append({
                    'sum_id': result[0],
                    'sum_value': result[1],
                    'count': result[2],
                })
                
                cur.execute("SELECT id, value FROM test_concurrent_read WHERE id % 1000 = 1 ORDER BY id")
                sample_rows = cur.fetchall()
                local_results[-1]['sample_count'] = len(sample_rows)

            results_queue.put((thread_id, local_results))
        except Exception as e:
            errors_queue.put((thread_id, str(e)))
        finally:
            cur.close()
            conn.close()
    
    threads = []
    for i in range(n_threads):
        t = threading.Thread(target=reader_thread, args=(i,))
        threads.append(t)
        t.start()
    
    for t in threads:
        t.join()

    thread_errors = []
    while not errors_queue.empty():
        thread_errors.append(errors_queue.get())
    if thread_errors:
        pytest.fail(f"Reader thread errors: {thread_errors}")
    
    all_results = {}
    while not results_queue.empty():
        thread_id, results = results_queue.get()
        all_results[thread_id] = results
    assert len(all_results) == n_threads, (
        f"Expected {n_threads} successful reader threads, got {len(all_results)}"
    )
    
    for thread_id, results in all_results.items():
        for i, result in enumerate(results):
            assert result['sum_id'] == expected_sum, (
                f"Thread {thread_id} iteration {i}: sum_id mismatch "
                f"{result['sum_id']} vs {expected_sum}"
            )
            assert result['sum_value'] == expected_value_sum, (
                f"Thread {thread_id} iteration {i}: sum_value mismatch "
                f"{result['sum_value']} vs {expected_value_sum}"
            )
            assert result['count'] == n_rows, (
                f"Thread {thread_id} iteration {i}: count mismatch "
                f"{result['count']} vs {n_rows}"
            )
            assert result['sample_count'] == expected_sample_count, (
                f"Thread {thread_id} iteration {i}: sample_count mismatch"
            )


@pytest.mark.skipif(not USE_LFC, reason="LFC is disabled, skipping")
def test_concurrent_write_read_with_lfc(neon_simple_env: NeonEnv):
    """
    验证并发写入和读取时 LFC 在 repeatable read 隔离级别下的正确性。

    Writer 持续提交 UPDATE。Reader 在显式 REPEATABLE READ 事务内多次读取，
    断言同一事务中快照一致（count 和 sum 不变）。
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

    main_cur.execute(
        "CREATE TABLE test_concurrent_rw "
        "(id int4 PRIMARY KEY, counter int, data text default repeat('y', 50))"
    )
    n_initial_rows = 1000
    main_cur.execute(
        f"INSERT INTO test_concurrent_rw (id, counter) "
        f"SELECT g, 0 FROM generate_series(1, {n_initial_rows}) g"
    )

    stop_event = threading.Event()
    errors_queue: queue.Queue = queue.Queue()

    def writer_thread():
        conn = endpoint.connect()
        conn.autocommit = True
        cur = conn.cursor()

        while not stop_event.is_set():
            row_id = random.randint(1, n_initial_rows)
            try:
                cur.execute(
                    f"UPDATE test_concurrent_rw SET counter = counter + 1 WHERE id = {row_id}"
                )
            except Exception as e:
                if "deadlock" not in str(e).lower():
                    errors_queue.put(("writer", str(e)))

        cur.close()
        conn.close()

    def reader_thread(thread_id: int):
        conn = endpoint.connect()
        cur = conn.cursor()

        for iteration in range(10):
            try:
                cur.execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ")

                cur.execute("SELECT count(*), sum(counter) FROM test_concurrent_rw")
                first = cur.fetchone()
                first_count, first_sum = first[0], first[1]

                time.sleep(0.05)

                cur.execute("SELECT count(*), sum(counter) FROM test_concurrent_rw")
                second = cur.fetchone()

                cur.execute("COMMIT")

                if second[0] != first_count or second[1] != first_sum:
                    errors_queue.put((
                        "reader",
                        f"Thread {thread_id} iter {iteration}: snapshot inconsistency "
                        f"first=({first_count},{first_sum}) "
                        f"second=({second[0]},{second[1]})",
                    ))

                assert first_count == n_initial_rows, (
                    f"Thread {thread_id}: row count changed to {first_count}"
                )
            except Exception as e:
                try:
                    cur.execute("ROLLBACK")
                except Exception:
                    pass
                if "could not serialize" not in str(e).lower():
                    errors_queue.put(("reader", str(e)))

        cur.close()
        conn.close()

    writer = threading.Thread(target=writer_thread)
    readers = [
        threading.Thread(target=reader_thread, args=(i,)) for i in range(3)
    ]

    writer.start()
    for r in readers:
        r.start()

    time.sleep(3)
    stop_event.set()

    writer.join()
    for r in readers:
        r.join()

    thread_errors = []
    while not errors_queue.empty():
        thread_errors.append(errors_queue.get())
    if thread_errors:
        pytest.fail(f"Errors: {thread_errors}")

    main_cur.execute("SELECT count(*), sum(counter) FROM test_concurrent_rw")
    final_result = main_cur.fetchone()
    assert final_result[0] == n_initial_rows, "Row count should remain constant"
    assert final_result[1] > 0, "Writer should have committed at least some updates"


@pytest.mark.skipif(not USE_LFC, reason="LFC is disabled, skipping")
def test_concurrent_prefetch_reads(neon_simple_env: NeonEnv):
    """
    验证并发预取场景下的数据正确性
    
    步骤：
    1. 创建大表
    2. 多个线程同时执行带预取的查询
    3. 验证每个查询结果正确
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
    
    main_cur.execute("""
        CREATE TABLE test_concurrent_prefetch (
            id int4 PRIMARY KEY,
            category int4,
            value numeric,
            data text default repeat('z', 100)
        )
    """)
    main_cur.execute("CREATE INDEX idx_category ON test_concurrent_prefetch(category)")
    
    n_rows = 10000
    main_cur.execute(
        f"INSERT INTO test_concurrent_prefetch (id, category, value) "
        f"SELECT g, g % 50, g * 1.5 FROM generate_series(1, {n_rows}) g"
    )
    main_cur.execute("ANALYZE test_concurrent_prefetch")
    
    results_queue: queue.Queue = queue.Queue()
    errors_queue: queue.Queue = queue.Queue()
    n_threads = 5
    
    def prefetch_reader(thread_id: int):
        conn = endpoint.connect()
        cur = conn.cursor()
        try:
            conn.set_session(autocommit=True)
            cur.execute("SET enable_seqscan=off")
            
            results = []
            for cat in range(10):
                category = (thread_id * 10 + cat) % 50
                cur.execute(
                    f"SELECT count(*), sum(value), min(id), max(id) "
                    f"FROM test_concurrent_prefetch WHERE category = {category}"
                )
                result = cur.fetchone()
                results.append({
                    'category': category,
                    'count': result[0],
                    'sum_value': result[1],
                    'min_id': result[2],
                    'max_id': result[3],
                })
            
            results_queue.put((thread_id, results))
        except Exception as e:
            errors_queue.put((thread_id, str(e)))
        finally:
            cur.close()
            conn.close()
    
    threads = []
    for i in range(n_threads):
        t = threading.Thread(target=prefetch_reader, args=(i,))
        threads.append(t)
        t.start()
    
    for t in threads:
        t.join()

    thread_errors = []
    while not errors_queue.empty():
        thread_errors.append(errors_queue.get())
    if thread_errors:
        pytest.fail(f"Prefetch reader thread errors: {thread_errors}")
    
    all_results = {}
    while not results_queue.empty():
        thread_id, results = results_queue.get()
        all_results[thread_id] = results
    assert len(all_results) == n_threads, (
        f"Expected {n_threads} successful prefetch reader threads, got {len(all_results)}"
    )
    
    for thread_id, results in all_results.items():
        for result in results:
            category = result['category']
            count = result['count']
            
            expected_count = n_rows // 50
            if category < n_rows % 50:
                expected_count += 1
            
            assert count == expected_count, (
                f"Thread {thread_id} category {category}: count mismatch "
                f"{count} vs {expected_count}"
            )
            
            if count > 0:
                min_id = result['min_id']
                max_id = result['max_id']
                assert min_id % 50 == category, f"Category mismatch for min_id={min_id}"
                assert max_id % 50 == category, f"Category mismatch for max_id={max_id}"


@pytest.mark.skipif(not USE_LFC, reason="LFC is disabled, skipping")
def test_concurrent_mixed_operations(neon_simple_env: NeonEnv):
    """
    验证并发混合操作（SELECT/INSERT/UPDATE/DELETE）的数据正确性
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
    
    main_cur.execute("CREATE TABLE test_mixed_ops (id int4 PRIMARY KEY, value int)")
    n_initial_rows = 1000
    main_cur.execute(
        f"INSERT INTO test_mixed_ops (id, value) "
        f"SELECT g, g FROM generate_series(1, {n_initial_rows}) g"
    )
    
    stop_event = threading.Event()
    errors_queue: queue.Queue = queue.Queue()
    stats_queue: queue.Queue = queue.Queue()
    
    def select_thread():
        conn = endpoint.connect()
        cur = conn.cursor()
        select_count = 0
        
        while not stop_event.is_set():
            try:
                cur.execute("SELECT count(*) FROM test_mixed_ops")
                cur.fetchone()
                select_count += 1
            except Exception as e:
                errors_queue.put(('select', str(e)))
        
        stats_queue.put(('select', select_count))
        cur.close()
        conn.close()
    
    def insert_thread():
        conn = endpoint.connect()
        cur = conn.cursor()
        insert_count = 0
        next_id = n_initial_rows + 1
        
        while not stop_event.is_set():
            try:
                cur.execute(f"INSERT INTO test_mixed_ops (id, value) VALUES ({next_id}, {next_id})")
                insert_count += 1
                next_id += 1
            except Exception as e:
                errors_queue.put(('insert', str(e)))
        
        stats_queue.put(('insert', insert_count))
        cur.close()
        conn.close()
    
    def update_thread():
        conn = endpoint.connect()
        cur = conn.cursor()
        update_count = 0
        
        while not stop_event.is_set():
            try:
                id = random.randint(1, n_initial_rows)
                cur.execute(f"UPDATE test_mixed_ops SET value = value + 1 WHERE id = {id}")
                update_count += 1
            except Exception as e:
                errors_queue.put(('update', str(e)))
        
        stats_queue.put(('update', update_count))
        cur.close()
        conn.close()
    
    threads = [
        threading.Thread(target=select_thread),
        threading.Thread(target=insert_thread),
        threading.Thread(target=update_thread),
    ]
    
    for t in threads:
        t.start()
    
    time.sleep(5)
    stop_event.set()
    
    for t in threads:
        t.join()
    
    critical_errors = []
    while not errors_queue.empty():
        source, error = errors_queue.get()
        if 'duplicate key' not in error.lower() and 'deadlock' not in error.lower():
            critical_errors.append((source, error))
    
    if critical_errors:
        pytest.fail(f"Critical errors: {critical_errors}")
    
    main_cur.execute("SELECT count(*) FROM test_mixed_ops")
    final_count = main_cur.fetchone()[0]
    assert final_count >= n_initial_rows, f"Final count should be at least {n_initial_rows}, got {final_count}"
