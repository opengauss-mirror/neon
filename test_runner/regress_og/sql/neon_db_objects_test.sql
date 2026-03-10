-- ============================================================================
-- Database Objects Test
-- ============================================================================
-- Description: Test database object operations with branching
-- ============================================================================

-- ============================================================================
-- Test Case 66: Table and Index Inheritance
-- ============================================================================

-- Step 1: Create test table with indexes on MAIN
CREATE TABLE test_db_obj_66 (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    value INTEGER,
    status VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes
CREATE INDEX idx_66_name ON test_db_obj_66(name);
CREATE INDEX idx_66_value ON test_db_obj_66(value);
CREATE INDEX idx_66_status ON test_db_obj_66(status);

-- Insert test data
INSERT INTO test_db_obj_66 (name, value, status) VALUES 
    ('main_record_1', 100, 'active'),
    ('main_record_2', 200, 'active'),
    ('main_record_3', 300, 'inactive');

-- Verify data on MAIN
SELECT 'MAIN branch - Initial data:' AS checkpoint;
SELECT id, name, value, status FROM test_db_obj_66 ORDER BY id;

-- Verify indexes exist on MAIN
SELECT 'MAIN branch - Indexes:' AS checkpoint;
SELECT indexname 
FROM pg_indexes 
WHERE tablename = 'test_db_obj_66' 
ORDER BY indexname;

-- Step 2: Create branch
\! neon_local timeline branch --branch-name test_branch_66 --ancestor-branch-name main

-- Start branch endpoint
\! neon_local endpoint create ep_test_66 --branch-name test_branch_66 --pg-port ${BRANCH1_PORT}

\! neon_local endpoint start ep_test_66

-- Step 3: Switch to branch and verify inheritance
\c - - - :branch1_port

SELECT 'BRANCH - Inherited data:' AS checkpoint;
SELECT id, name, value, status FROM test_db_obj_66 ORDER BY id;

-- Verify indexes inherited
SELECT 'BRANCH - Inherited indexes:' AS checkpoint;
SELECT indexname 
FROM pg_indexes 
WHERE tablename = 'test_db_obj_66' 
ORDER BY indexname;

-- Verify sequence inherited and working
SELECT 'BRANCH - Sequence test:' AS checkpoint;
INSERT INTO test_db_obj_66 (name, value, status) 
VALUES ('branch_record_1', 400, 'active');

SELECT id, name, value, status 
FROM test_db_obj_66 
WHERE name LIKE 'branch%' 
ORDER BY id;

-- Step 4: Add branch-specific objects
-- Add new index on branch
CREATE INDEX idx_66_branch_combo ON test_db_obj_66(status, value);

-- Add more data on branch
INSERT INTO test_db_obj_66 (name, value, status) VALUES 
    ('branch_record_2', 500, 'pending'),
    ('branch_record_3', 600, 'pending');

SELECT 'BRANCH - All data after additions:' AS checkpoint;
SELECT id, name, value, status FROM test_db_obj_66 ORDER BY id;

SELECT 'BRANCH - All indexes (including branch-specific):' AS checkpoint;
SELECT indexname 
FROM pg_indexes 
WHERE tablename = 'test_db_obj_66' 
ORDER BY indexname;

-- Step 5: Switch back to MAIN and verify isolation
\c - - - :main_port

SELECT 'MAIN - Data after branch operations:' AS checkpoint;
SELECT id, name, value, status FROM test_db_obj_66 ORDER BY id;

-- Verify branch-specific index NOT on MAIN
SELECT 'MAIN - Indexes (should NOT include branch index):' AS checkpoint;
SELECT indexname 
FROM pg_indexes 
WHERE tablename = 'test_db_obj_66' 
ORDER BY indexname;

-- Verify no branch data on MAIN
SELECT 'MAIN - Branch data isolation check:' AS checkpoint;
SELECT COUNT(*) AS branch_records_on_main 
FROM test_db_obj_66 
WHERE name LIKE 'branch%';

-- Step 6: Cleanup
-- Drop table on MAIN
DROP TABLE test_db_obj_66;

-- Stop branch endpoint
\! neon_local endpoint stop ep_test_66
\! sleep 2

-- ============================================================================
-- Test Case 67: Sequence Inheritance and Independence
-- ============================================================================

-- Step 1: Create sequence on MAIN
CREATE SEQUENCE test_seq_67 START WITH 1000 INCREMENT BY 10;

-- Use sequence on MAIN
SELECT 'MAIN - Initial sequence values:' AS checkpoint;
SELECT nextval('test_seq_67') AS seq_val_1;
SELECT nextval('test_seq_67') AS seq_val_2;
SELECT currval('test_seq_67') AS current_val;

-- Step 2: Create branch
\! neon_local timeline branch --branch-name test_branch_67 --ancestor-branch-name main
\! neon_local endpoint create ep_test_67 --branch-name test_branch_67 --pg-port ${BRANCH1_PORT}

\! neon_local endpoint start ep_test_67

-- Step 3: Use sequence on branch
\c - - - :branch1_port

SELECT 'BRANCH - Sequence after inheritance:' AS checkpoint;
SELECT nextval('test_seq_67') AS seq_val_branch_1;
SELECT nextval('test_seq_67') AS seq_val_branch_2;

-- Step 4: Verify independence on MAIN
\c - - - :main_port

SELECT 'MAIN - Sequence after branch operations:' AS checkpoint;
SELECT nextval('test_seq_67') AS main_next_val;

-- Cleanup
DROP SEQUENCE test_seq_67;
\! neon_local endpoint stop ep_test_67
\! sleep 2

-- ============================================================================
-- Test Case 68: View Inheritance
-- ============================================================================

-- Step 1: Create table and view on MAIN
CREATE TABLE test_base_68 (
    id SERIAL PRIMARY KEY,
    category VARCHAR(50),
    amount NUMERIC(10,2)
);

INSERT INTO test_base_68 (category, amount) VALUES 
    ('A', 100.00),
    ('B', 200.00),
    ('A', 150.00);

CREATE VIEW test_view_68 AS 
SELECT category, SUM(amount) AS total_amount 
FROM test_base_68 
GROUP BY category;

SELECT 'MAIN - View data:' AS checkpoint;
SELECT * FROM test_view_68 ORDER BY category;

-- Step 2: Create branch
\! neon_local timeline branch --branch-name test_branch_68 --ancestor-branch-name main
\! neon_local endpoint create ep_test_68 --branch-name test_branch_68 --pg-port ${BRANCH1_PORT}

\! neon_local endpoint start ep_test_68

-- Step 3: Verify view on branch
\c - - - :branch1_port

SELECT 'BRANCH - Inherited view:' AS checkpoint;
SELECT * FROM test_view_68 ORDER BY category;

-- Add data on branch
INSERT INTO test_base_68 (category, amount) VALUES ('C', 300.00);

SELECT 'BRANCH - View after insert:' AS checkpoint;
SELECT * FROM test_view_68 ORDER BY category;

-- Step 4: Verify isolation on MAIN
\c - - - :main_port

SELECT 'MAIN - View after branch operations:' AS checkpoint;
SELECT * FROM test_view_68 ORDER BY category;

-- Cleanup
DROP VIEW test_view_68;
DROP TABLE test_base_68;
\! neon_local endpoint stop ep_test_68
\! sleep 2

-- ============================================================================
-- Test Case 69: Constraint Inheritance
-- ============================================================================

-- Step 1: Create table with constraints on MAIN
CREATE TABLE test_constraint_69 (
    id SERIAL PRIMARY KEY,
    email VARCHAR(100) UNIQUE NOT NULL,
    age INTEGER CHECK (age >= 0 AND age <= 150),
    status VARCHAR(20) DEFAULT 'active'
);

INSERT INTO test_constraint_69 (email, age) VALUES 
    ('user1@test.com', 25),
    ('user2@test.com', 30);

SELECT 'MAIN - Initial data:' AS checkpoint;
SELECT * FROM test_constraint_69 ORDER BY id;

-- Step 2: Create branch
\! neon_local timeline branch --branch-name test_branch_69 --ancestor-branch-name main
\! neon_local endpoint create ep_test_69 --branch-name test_branch_69 --pg-port ${BRANCH1_PORT}

\! neon_local endpoint start ep_test_69

-- Step 3: Test constraints on branch
\c - - - :branch1_port

SELECT 'BRANCH - Testing constraints:' AS checkpoint;

-- This should succeed
INSERT INTO test_constraint_69 (email, age) VALUES ('user3@test.com', 35);

-- This should fail (duplicate email) - expect error
INSERT INTO test_constraint_69 (email, age) VALUES ('user1@test.com', 40);

SELECT 'BRANCH - Data after operations:' AS checkpoint;
SELECT * FROM test_constraint_69 ORDER BY id;

-- Step 4: Verify MAIN unchanged
\c - - - :main_port

SELECT 'MAIN - Data after branch operations:' AS checkpoint;
SELECT COUNT(*) AS record_count FROM test_constraint_69;

-- Cleanup
DROP TABLE test_constraint_69;
\! neon_local endpoint stop ep_test_69
\! sleep 2

-- ============================================================================
-- Test Case 70: Trigger Inheritance
-- ============================================================================

-- Step 1: Create table with trigger on MAIN
CREATE TABLE test_trigger_70 (
    id SERIAL PRIMARY KEY,
    value INTEGER,
    last_updated TIMESTAMP
);

CREATE OR REPLACE FUNCTION update_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.last_updated = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_update_timestamp
BEFORE INSERT OR UPDATE ON test_trigger_70
FOR EACH ROW EXECUTE FUNCTION update_timestamp();

INSERT INTO test_trigger_70 (value) VALUES (100);

SELECT 'MAIN - Data with trigger:' AS checkpoint;
SELECT id, value, last_updated IS NOT NULL AS has_timestamp FROM test_trigger_70;

-- Step 2: Create branch
\! neon_local timeline branch --branch-name test_branch_70 --ancestor-branch-name main
\! neon_local endpoint create ep_test_70 --branch-name test_branch_70 --pg-port ${BRANCH1_PORT}

\! neon_local endpoint start ep_test_70

-- Step 3: Test trigger on branch
\c - - - :branch1_port

INSERT INTO test_trigger_70 (value) VALUES (200);

SELECT 'BRANCH - Trigger working:' AS checkpoint;
SELECT id, value, last_updated IS NOT NULL AS has_timestamp FROM test_trigger_70 ORDER BY id;

-- Step 4: Verify MAIN unchanged
\c - - - :main_port

SELECT 'MAIN - Data after branch operations:' AS checkpoint;
SELECT COUNT(*) AS record_count FROM test_trigger_70;

-- Cleanup
DROP TRIGGER trg_update_timestamp ON test_trigger_70;
DROP FUNCTION update_timestamp();
DROP TABLE test_trigger_70;
\! neon_local endpoint stop ep_test_70
\! sleep 2

-- ============================================================================
-- Test Case 71: Foreign Key Inheritance
-- ============================================================================

-- Step 1: Create tables with foreign key on MAIN
CREATE TABLE test_parent_71 (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50)
);

CREATE TABLE test_child_71 (
    id SERIAL PRIMARY KEY,
    parent_id INTEGER REFERENCES test_parent_71(id),
    description VARCHAR(100)
);

INSERT INTO test_parent_71 (name) VALUES ('Parent1'), ('Parent2');
INSERT INTO test_child_71 (parent_id, description) VALUES (1, 'Child1'), (1, 'Child2');

SELECT 'MAIN - Parent-child data:' AS checkpoint;
SELECT p.name, COUNT(c.id) AS child_count 
FROM test_parent_71 p 
LEFT JOIN test_child_71 c ON p.id = c.parent_id 
GROUP BY p.name ORDER BY p.name;

-- Step 2: Create branch
\! neon_local timeline branch --branch-name test_branch_71 --ancestor-branch-name main
\! neon_local endpoint create ep_test_71 --branch-name test_branch_71 --pg-port ${BRANCH1_PORT}

\! neon_local endpoint start ep_test_71

-- Step 3: Test foreign key on branch
\c - - - :branch1_port

INSERT INTO test_parent_71 (name) VALUES ('Parent3');
INSERT INTO test_child_71 (parent_id, description) VALUES (3, 'Child3');

-- This should fail (invalid parent_id) - expect error
INSERT INTO test_child_71 (parent_id, description) VALUES (999, 'Invalid');

SELECT 'BRANCH - Data after operations:' AS checkpoint;
SELECT COUNT(*) AS parent_count FROM test_parent_71;
SELECT COUNT(*) AS child_count FROM test_child_71;

-- Step 4: Verify MAIN unchanged
\c - - - :main_port

SELECT 'MAIN - Data after branch operations:' AS checkpoint;
SELECT COUNT(*) AS parent_count FROM test_parent_71;
SELECT COUNT(*) AS child_count FROM test_child_71;

-- Cleanup
DROP TABLE test_child_71;
DROP TABLE test_parent_71;
\! neon_local endpoint stop ep_test_71
\! sleep 2

-- ============================================================================
-- Test Case 72: Schema Inheritance
-- ============================================================================

-- Step 1: Create schema and objects on MAIN
CREATE SCHEMA test_schema_72;

CREATE TABLE test_schema_72.data_table (
    id SERIAL PRIMARY KEY,
    info VARCHAR(100)
);

INSERT INTO test_schema_72.data_table (info) VALUES ('Main data');

SELECT 'MAIN - Schema data:' AS checkpoint;
SELECT * FROM test_schema_72.data_table;

-- Step 2: Create branch
\! neon_local timeline branch --branch-name test_branch_72 --ancestor-branch-name main
\! neon_local endpoint create ep_test_72 --branch-name test_branch_72 --pg-port ${BRANCH1_PORT}

\! neon_local endpoint start ep_test_72

-- Step 3: Verify schema on branch
\c - - - :branch1_port

SELECT 'BRANCH - Inherited schema:' AS checkpoint;
SELECT * FROM test_schema_72.data_table;

INSERT INTO test_schema_72.data_table (info) VALUES ('Branch data');

SELECT 'BRANCH - Data after insert:' AS checkpoint;
SELECT COUNT(*) AS record_count FROM test_schema_72.data_table;

-- Step 4: Verify MAIN unchanged
\c - - - :main_port

SELECT 'MAIN - Data after branch operations:' AS checkpoint;
SELECT COUNT(*) AS record_count FROM test_schema_72.data_table;

-- Cleanup
DROP SCHEMA test_schema_72 CASCADE;
\! neon_local endpoint stop ep_test_72
\! sleep 2

-- ============================================================================
-- Test Case 73: Composite Index Inheritance
-- ============================================================================

-- Step 1: Create table with composite indexes on MAIN
CREATE TABLE test_composite_73 (
    id SERIAL PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    age INTEGER,
    city VARCHAR(50)
);

CREATE INDEX idx_73_name ON test_composite_73(last_name, first_name);
CREATE INDEX idx_73_location ON test_composite_73(city, age);

INSERT INTO test_composite_73 (first_name, last_name, age, city) VALUES 
    ('John', 'Doe', 30, 'NYC'),
    ('Jane', 'Smith', 25, 'LA');

SELECT 'MAIN - Composite indexes:' AS checkpoint;
SELECT indexname 
FROM pg_indexes 
WHERE tablename = 'test_composite_73' 
ORDER BY indexname;

-- Step 2: Create branch
\! neon_local timeline branch --branch-name test_branch_73 --ancestor-branch-name main
\! neon_local endpoint create ep_test_73 --branch-name test_branch_73 --pg-port ${BRANCH1_PORT}

\! neon_local endpoint start ep_test_73

-- Step 3: Verify indexes on branch
\c - - - :branch1_port

SELECT 'BRANCH - Inherited indexes:' AS checkpoint;
SELECT indexname 
FROM pg_indexes 
WHERE tablename = 'test_composite_73' 
ORDER BY indexname;

-- Add branch-specific index
CREATE INDEX idx_73_branch_age ON test_composite_73(age);

SELECT 'BRANCH - All indexes:' AS checkpoint;
SELECT COUNT(*) AS index_count 
FROM pg_indexes 
WHERE tablename = 'test_composite_73';

-- Step 4: Verify MAIN unchanged
\c - - - :main_port

SELECT 'MAIN - Indexes after branch operations:' AS checkpoint;
SELECT COUNT(*) AS index_count 
FROM pg_indexes 
WHERE tablename = 'test_composite_73';

-- Cleanup
DROP TABLE test_composite_73;
\! neon_local endpoint stop ep_test_73
\! sleep 2

-- ============================================================================
-- Test Case 74: Partial Index Inheritance
-- ============================================================================

-- Step 1: Create table with partial index on MAIN
CREATE TABLE test_partial_74 (
    id SERIAL PRIMARY KEY,
    status VARCHAR(20),
    value INTEGER
);

CREATE INDEX idx_74_active ON test_partial_74(value) WHERE status = 'active';

INSERT INTO test_partial_74 (status, value) VALUES 
    ('active', 100),
    ('inactive', 200),
    ('active', 300);

SELECT 'MAIN - Partial index data:' AS checkpoint;
SELECT * FROM test_partial_74 ORDER BY id;

-- Step 2: Create branch
\! neon_local timeline branch --branch-name test_branch_74 --ancestor-branch-name main
\! neon_local endpoint create ep_test_74 --branch-name test_branch_74 --pg-port ${BRANCH1_PORT}

\! neon_local endpoint start ep_test_74

-- Step 3: Verify partial index on branch
\c - - - :branch1_port

SELECT 'BRANCH - Inherited partial index:' AS checkpoint;
SELECT indexname, indexdef 
FROM pg_indexes 
WHERE tablename = 'test_partial_74' AND indexname = 'idx_74_active';

INSERT INTO test_partial_74 (status, value) VALUES ('active', 400);

SELECT 'BRANCH - Data after insert:' AS checkpoint;
SELECT COUNT(*) AS active_count FROM test_partial_74 WHERE status = 'active';

-- Step 4: Verify MAIN unchanged
\c - - - :main_port

SELECT 'MAIN - Data after branch operations:' AS checkpoint;
SELECT COUNT(*) AS active_count FROM test_partial_74 WHERE status = 'active';

-- Cleanup
DROP TABLE test_partial_74;
\! neon_local endpoint stop ep_test_74
\! sleep 2

-- ============================================================================
-- Test Case 75: Multiple Object Types Together
-- ============================================================================

-- Step 1: Create multiple object types on MAIN
CREATE TABLE test_multi_75 (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50),
    value INTEGER
);

CREATE INDEX idx_75_name ON test_multi_75(name);
CREATE SEQUENCE test_seq_75 START WITH 100;
CREATE VIEW test_view_75 AS SELECT name, COUNT(*) AS count FROM test_multi_75 GROUP BY name;

INSERT INTO test_multi_75 (name, value) VALUES 
    ('TypeA', 10),
    ('TypeB', 20),
    ('TypeA', 30);

SELECT 'MAIN - Multiple objects:' AS checkpoint;
SELECT * FROM test_view_75 ORDER BY name;

-- Step 2: Create branch
\! neon_local timeline branch --branch-name test_branch_75 --ancestor-branch-name main
\! neon_local endpoint create ep_test_75 --branch-name test_branch_75 --pg-port ${BRANCH1_PORT}

\! neon_local endpoint start ep_test_75

-- Step 3: Use all objects on branch
\c - - - :branch1_port

SELECT 'BRANCH - All inherited objects:' AS checkpoint;
SELECT * FROM test_view_75 ORDER BY name;
SELECT nextval('test_seq_75') AS seq_value;

INSERT INTO test_multi_75 (name, value) VALUES ('TypeC', 40);

SELECT 'BRANCH - After modifications:' AS checkpoint;
SELECT * FROM test_view_75 ORDER BY name;

-- Step 4: Verify MAIN unchanged
\c - - - :main_port

SELECT 'MAIN - Data after branch operations:' AS checkpoint;
SELECT COUNT(*) AS record_count FROM test_multi_75;
SELECT COUNT(*) AS view_count FROM test_view_75;

-- Cleanup
DROP VIEW test_view_75;
DROP SEQUENCE test_seq_75;
DROP TABLE test_multi_75;
\! neon_local endpoint stop ep_test_75
\! sleep 2
