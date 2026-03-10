-- ============================================================================
-- Large Data Volume Test
-- ============================================================================
-- Description: Test large data volume operations with branching
-- Tests data inheritance, isolation, and performance with large datasets
-- ============================================================================

-- ============================================================================
-- Test Case 76: Large Table Data Inheritance
-- ============================================================================

-- Step 1: Create large table on MAIN
CREATE TABLE test_large_76 (
    id SERIAL PRIMARY KEY,
    data TEXT,
    value INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert 10,000 rows
INSERT INTO test_large_76 (data, value)
SELECT 
    'data_' || i::TEXT,
    i
FROM generate_series(1, 10000) AS i;

-- Verify row count on MAIN
SELECT 'MAIN branch - Row count:' AS checkpoint;
SELECT COUNT(*) AS row_count FROM test_large_76;

-- Verify data sample
SELECT 'MAIN branch - Data sample:' AS checkpoint;
SELECT id, data, value 
FROM test_large_76 
WHERE id IN (1, 5000, 10000)
ORDER BY id;

-- Step 2: Create branch
\! neon_local timeline branch --branch-name test_branch_76 --ancestor-branch-name main
\! neon_local endpoint create ep_test_76 --branch-name test_branch_76 --pg-port ${BRANCH1_PORT}
\! neon_local endpoint start ep_test_76

-- Step 3: Verify data inherited on branch
\c - - - :branch1_port

SELECT 'BRANCH - Inherited row count:' AS checkpoint;
SELECT COUNT(*) AS row_count FROM test_large_76;

SELECT 'BRANCH - Inherited data sample:' AS checkpoint;
SELECT id, data, value 
FROM test_large_76 
WHERE id IN (1, 5000, 10000)
ORDER BY id;

-- Step 4: Add more data on branch
INSERT INTO test_large_76 (data, value)
SELECT 
    'branch_data_' || i::TEXT,
    i + 10000
FROM generate_series(1, 5000) AS i;

SELECT 'BRANCH - Row count after insert:' AS checkpoint;
SELECT COUNT(*) AS row_count FROM test_large_76;

-- Step 5: Verify MAIN isolation
\c - - - :main_port

SELECT 'MAIN - Row count (should be unchanged):' AS checkpoint;
SELECT COUNT(*) AS row_count FROM test_large_76;

-- Cleanup
DROP TABLE test_large_76;
\! neon_local endpoint stop ep_test_76
\! sleep 2

-- ============================================================================
-- Test Case 77: Large Data with Index
-- ============================================================================

-- Step 1: Create table with index on MAIN
CREATE TABLE test_indexed_77 (
    id SERIAL PRIMARY KEY,
    category VARCHAR(50),
    value INTEGER,
    description TEXT
);

-- Create index
CREATE INDEX idx_77_category ON test_indexed_77(category);
CREATE INDEX idx_77_value ON test_indexed_77(value);

-- Insert 10,000 rows with 10 categories
INSERT INTO test_indexed_77 (category, value, description)
SELECT 
    'category_' || (i % 10)::TEXT,
    i,
    'description_' || i::TEXT
FROM generate_series(1, 10000) AS i;

-- Verify index usage on MAIN
SELECT 'MAIN branch - Category distribution:' AS checkpoint;
SELECT category, COUNT(*) AS count
FROM test_indexed_77
GROUP BY category
ORDER BY category;

-- Step 2: Create branch
\! neon_local timeline branch --branch-name test_branch_77 --ancestor-branch-name main
\! neon_local endpoint create ep_test_77 --branch-name test_branch_77 --pg-port ${BRANCH1_PORT}
\! neon_local endpoint start ep_test_77

-- Step 3: Verify index on branch
\c - - - :branch1_port

SELECT 'BRANCH - Inherited category distribution:' AS checkpoint;
SELECT category, COUNT(*) AS count
FROM test_indexed_77
GROUP BY category
ORDER BY category;

-- Step 4: Add data on branch
INSERT INTO test_indexed_77 (category, value, description)
SELECT 
    'branch_category_' || (i % 5)::TEXT,
    i + 10000,
    'branch_description_' || i::TEXT
FROM generate_series(1, 5000) AS i;

SELECT 'BRANCH - All categories after insert:' AS checkpoint;
SELECT category, COUNT(*) AS count
FROM test_indexed_77
GROUP BY category
ORDER BY category;

-- Step 5: Verify MAIN isolation
\c - - - :main_port

SELECT 'MAIN - Category distribution (unchanged):' AS checkpoint;
SELECT category, COUNT(*) AS count
FROM test_indexed_77
GROUP BY category
ORDER BY category;

-- Cleanup
DROP TABLE test_indexed_77;
\! neon_local endpoint stop ep_test_77
\! sleep 2

-- ============================================================================
-- Test Case 78: Large Data Update Performance
-- ============================================================================

-- Step 1: Create table on MAIN
CREATE TABLE test_update_78 (
    id SERIAL PRIMARY KEY,
    status VARCHAR(20),
    counter INTEGER DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert 10,000 rows
INSERT INTO test_update_78 (status, counter)
SELECT 
    CASE WHEN i % 2 = 0 THEN 'active' ELSE 'inactive' END,
    0
FROM generate_series(1, 10000) AS i;

-- Verify initial state
SELECT 'MAIN branch - Initial status distribution:' AS checkpoint;
SELECT status, COUNT(*) AS count, AVG(counter) AS avg_counter
FROM test_update_78
GROUP BY status
ORDER BY status;

-- Step 2: Create branch
\! neon_local timeline branch --branch-name test_branch_78 --ancestor-branch-name main
\! neon_local endpoint create ep_test_78 --branch-name test_branch_78 --pg-port ${BRANCH1_PORT}
\! neon_local endpoint start ep_test_78

-- Step 3: Perform updates on branch
\c - - - :branch1_port

-- Update counters on branch
UPDATE test_update_78 
SET counter = counter + 1, updated_at = CURRENT_TIMESTAMP
WHERE status = 'active';

SELECT 'BRANCH - After update:' AS checkpoint;
SELECT status, COUNT(*) AS count, AVG(counter) AS avg_counter
FROM test_update_78
GROUP BY status
ORDER BY status;

-- Step 4: Verify MAIN unchanged
\c - - - :main_port

SELECT 'MAIN - Status distribution (unchanged):' AS checkpoint;
SELECT status, COUNT(*) AS count, AVG(counter) AS avg_counter
FROM test_update_78
GROUP BY status
ORDER BY status;

-- Cleanup
DROP TABLE test_update_78;
\! neon_local endpoint stop ep_test_78
\! sleep 2

-- ============================================================================
-- Test Case 79: Large Data Delete Operations
-- ============================================================================

-- Step 1: Create table on MAIN
CREATE TABLE test_delete_79 (
    id SERIAL PRIMARY KEY,
    category VARCHAR(50),
    value INTEGER,
    active BOOLEAN DEFAULT true
);

-- Insert 10,000 rows
INSERT INTO test_delete_79 (category, value, active)
SELECT 
    'category_' || (i % 10)::TEXT,
    i,
    true
FROM generate_series(1, 10000) AS i;

SELECT 'MAIN branch - Initial row count:' AS checkpoint;
SELECT COUNT(*) AS total_rows, COUNT(*) FILTER (WHERE active) AS active_rows
FROM test_delete_79;

-- Step 2: Create branch
\! neon_local timeline branch --branch-name test_branch_79 --ancestor-branch-name main
\! neon_local endpoint create ep_test_79 --branch-name test_branch_79 --pg-port ${BRANCH1_PORT}
\! neon_local endpoint start ep_test_79

-- Step 3: Delete data on branch
\c - - - :branch1_port

-- Soft delete (update)
UPDATE test_delete_79 
SET active = false 
WHERE value % 2 = 0;

SELECT 'BRANCH - After soft delete:' AS checkpoint;
SELECT COUNT(*) AS total_rows, COUNT(*) FILTER (WHERE active) AS active_rows
FROM test_delete_79;

-- Hard delete
DELETE FROM test_delete_79 WHERE value < 1000;

SELECT 'BRANCH - After hard delete:' AS checkpoint;
SELECT COUNT(*) AS total_rows, COUNT(*) FILTER (WHERE active) AS active_rows
FROM test_delete_79;

-- Step 4: Verify MAIN unchanged
\c - - - :main_port

SELECT 'MAIN - Row count (unchanged):' AS checkpoint;
SELECT COUNT(*) AS total_rows, COUNT(*) FILTER (WHERE active) AS active_rows
FROM test_delete_79;

-- Cleanup
DROP TABLE test_delete_79;
\! neon_local endpoint stop ep_test_79
\! sleep 2

-- ============================================================================
-- Test Case 80: Large Data with Multiple Branches
-- ============================================================================

-- Step 1: Create table on MAIN
CREATE TABLE test_multi_branch_80 (
    id SERIAL PRIMARY KEY,
    branch_name VARCHAR(50),
    data TEXT,
    value INTEGER
);

-- Insert initial data
INSERT INTO test_multi_branch_80 (branch_name, data, value)
SELECT 
    'main',
    'main_data_' || i::TEXT,
    i
FROM generate_series(1, 5000) AS i;

SELECT 'MAIN branch - Initial count:' AS checkpoint;
SELECT branch_name, COUNT(*) AS count
FROM test_multi_branch_80
GROUP BY branch_name;

-- Step 2: Create first branch
\! neon_local timeline branch --branch-name test_branch_80_a --ancestor-branch-name main
\! neon_local endpoint create ep_test_80_a --branch-name test_branch_80_a --pg-port ${BRANCH1_PORT}
\! neon_local endpoint start ep_test_80_a

-- Step 3: Add data on first branch
\c - - - :branch1_port

INSERT INTO test_multi_branch_80 (branch_name, data, value)
SELECT 
    'branch_a',
    'branch_a_data_' || i::TEXT,
    i + 5000
FROM generate_series(1, 3000) AS i;

SELECT 'BRANCH A - Data count:' AS checkpoint;
SELECT branch_name, COUNT(*) AS count
FROM test_multi_branch_80
GROUP BY branch_name
ORDER BY branch_name;

-- Step 4: Verify MAIN unchanged
\c - - - :main_port

SELECT 'MAIN - Data count (unchanged):' AS checkpoint;
SELECT branch_name, COUNT(*) AS count
FROM test_multi_branch_80
GROUP BY branch_name;

-- Cleanup
DROP TABLE test_multi_branch_80;
\! neon_local endpoint stop ep_test_80_a
\! sleep 2
