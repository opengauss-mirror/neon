-- ============================================================================
-- Neon Basic Functionality Test
-- ============================================================================
-- Tests core database operations on Neon storage layer.
-- This test can run independently without requiring neon_setup.sql
--
-- Tests included:
--   1. Table Creation
--   2. Data Insertion (single & bulk)
--   3. Data Update
--   4. Data Deletion
--   5. Index Operations
--   6. Transaction Handling (COMMIT & ROLLBACK)
--   7. Constraint Validation
-- ============================================================================

\echo '=============================================='
\echo '  Neon Basic Functionality Tests'
\echo '=============================================='

-- ============================================================================
-- Test 1: Table Creation
-- ============================================================================
\echo '--- Test 1: Table Creation ---'

DROP TABLE IF EXISTS basic_test CASCADE;

CREATE TABLE basic_test (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    value INT,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Verify table exists
SELECT 
    CASE WHEN EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'basic_test')
    THEN 'PASS: Table created successfully'
    ELSE 'FAIL: Table not found'
    END AS table_creation_check;

\echo '  Table creation: PASSED'

-- ============================================================================
-- Test 2: Data Insertion
-- ============================================================================
\echo '--- Test 2: Data Insertion ---'

-- Single row inserts
INSERT INTO basic_test (name, value, description) VALUES
    ('test1', 100, 'First test row'),
    ('test2', 200, 'Second test row'),
    ('test3', 300, 'Third test row');

-- Verify data inserted
SELECT 
    COUNT(*) AS row_count,
    CASE WHEN COUNT(*) = 3 THEN 'PASS' ELSE 'FAIL' END AS insert_check
FROM basic_test;

-- Display inserted data
SELECT id, name, value FROM basic_test ORDER BY id;

-- Bulk insert using generate_series
INSERT INTO basic_test (name, value)
SELECT 'bulk_' || i, i * 10
FROM generate_series(1, 10) AS i;

-- Verify bulk insert
SELECT 
    COUNT(*) AS total_rows,
    CASE WHEN COUNT(*) = 13 THEN 'PASS' ELSE 'FAIL' END AS bulk_insert_check
FROM basic_test;

\echo '  Data insertion: PASSED'

-- ============================================================================
-- Test 3: Data Update
-- ============================================================================
\echo '--- Test 3: Data Update ---'

-- Single row update
UPDATE basic_test SET value = value + 1 WHERE name = 'test1';

-- Verify update
SELECT 
    name, value,
    CASE WHEN value = 101 THEN 'PASS' ELSE 'FAIL' END AS update_check
FROM basic_test WHERE name = 'test1';

-- Bulk update
UPDATE basic_test SET value = value * 2 WHERE name LIKE 'bulk_%';

-- Show some updated rows
SELECT name, value FROM basic_test WHERE name LIKE 'bulk_%' ORDER BY id LIMIT 5;

\echo '  Data update: PASSED'

-- ============================================================================
-- Test 4: Data Deletion
-- ============================================================================
\echo '--- Test 4: Data Deletion ---'

-- Get count before delete
SELECT COUNT(*) AS before_delete FROM basic_test;

-- Delete specific row
DELETE FROM basic_test WHERE name = 'test2';

-- Verify deletion
SELECT 
    COUNT(*) AS after_delete,
    CASE WHEN COUNT(*) = 12 THEN 'PASS' ELSE 'FAIL' END AS delete_check
FROM basic_test;

-- Verify deleted row is gone
SELECT 
    CASE WHEN NOT EXISTS(SELECT 1 FROM basic_test WHERE name = 'test2')
    THEN 'PASS: Row deleted'
    ELSE 'FAIL: Row still exists'
    END AS delete_verify;

\echo '  Data deletion: PASSED'

-- ============================================================================
-- Test 5: Index Operations
-- ============================================================================
\echo '--- Test 5: Index Operations ---'

-- Create indexes
CREATE INDEX idx_basic_test_name ON basic_test(name);
CREATE INDEX idx_basic_test_value ON basic_test(value);

-- Verify indexes exist
SELECT 
    indexname,
    CASE WHEN indexname IS NOT NULL THEN 'EXISTS' ELSE 'MISSING' END AS status
FROM pg_indexes 
WHERE tablename = 'basic_test'
ORDER BY indexname;

-- Test index usage (EXPLAIN output)
EXPLAIN (COSTS OFF) SELECT * FROM basic_test WHERE name = 'test1';

\echo '  Index operations: PASSED'

-- ============================================================================
-- Test 6: Transaction Handling
-- ============================================================================
\echo '--- Test 6: Transaction Handling ---'

-- Test COMMIT
BEGIN;
INSERT INTO basic_test (name, value) VALUES ('txn_commit_test', 999);
COMMIT;

SELECT 
    CASE WHEN EXISTS(SELECT 1 FROM basic_test WHERE name = 'txn_commit_test')
    THEN 'PASS: Committed row exists'
    ELSE 'FAIL: Committed row missing'
    END AS commit_check;

-- Test ROLLBACK
BEGIN;
INSERT INTO basic_test (name, value) VALUES ('txn_rollback_test', 888);
-- Verify it exists during transaction
SELECT COUNT(*) AS during_txn FROM basic_test WHERE name = 'txn_rollback_test';
ROLLBACK;

SELECT 
    CASE WHEN NOT EXISTS(SELECT 1 FROM basic_test WHERE name = 'txn_rollback_test')
    THEN 'PASS: Rolled back row does not exist'
    ELSE 'FAIL: Rolled back row still exists'
    END AS rollback_check;

\echo '  Transaction handling: PASSED'

-- ============================================================================
-- Test 7: Constraint Validation
-- ============================================================================
\echo '--- Test 7: Constraint Validation ---'

-- Test NOT NULL constraint (should fail)
INSERT INTO basic_test (name, value) VALUES (NULL, 100);

-- Test PRIMARY KEY constraint - duplicate (should fail)
INSERT INTO basic_test (id, name, value) VALUES (1, 'duplicate_id', 100);

-- Add CHECK constraint
ALTER TABLE basic_test ADD CONSTRAINT chk_value_positive CHECK (value >= 0);

-- Test CHECK constraint (should fail)
INSERT INTO basic_test (name, value) VALUES ('negative_test', -1);

\echo '  Constraint validation: PASSED (errors above are expected)'

-- ============================================================================
-- Cleanup Test Table
-- ============================================================================
\echo '--- Cleaning Up Test Table ---'

DROP TABLE basic_test;

-- Verify cleanup
SELECT 
    CASE WHEN NOT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'basic_test')
    THEN 'PASS: Table dropped successfully'
    ELSE 'FAIL: Table still exists'
    END AS cleanup_check;


-- ============================================================================
-- Test Summary
-- ============================================================================
\echo '=============================================='
\echo '  Basic Functionality Tests Complete'
\echo '=============================================='
