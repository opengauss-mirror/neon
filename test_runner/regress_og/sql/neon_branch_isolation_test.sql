-- Neon Branch Isolation Test
-- Tests that CRUD operations on main and child branches are completely isolated

-- Setup: Create initial data on main branch
\c - - - :main_port
SET client_min_messages = WARNING;

DROP TABLE IF EXISTS isolation_test CASCADE;

CREATE TABLE isolation_test (
    id SERIAL PRIMARY KEY,
    category TEXT NOT NULL,
    value INTEGER NOT NULL,
    description TEXT
);

INSERT INTO isolation_test (category, value, description) VALUES
    ('initial', 1, 'Row 1'),
    ('initial', 2, 'Row 2'),
    ('initial', 3, 'Row 3'),
    ('initial', 4, 'Row 4'),
    ('initial', 5, 'Row 5');

SELECT * FROM isolation_test ORDER BY id;

-- Create child branch
\! neon_local timeline branch --branch-name isolation_test_branch
\! neon_local endpoint create ep-isolation-test --branch-name isolation_test_branch --pg-port ${BRANCH1_PORT}
\! neon_local endpoint start ep-isolation-test
\! sleep 5

-- Verify branch inherited data
\c - - - :branch1_port
SET client_min_messages = WARNING;
SELECT 'BRANCH - Inherited data:' AS info;
SELECT * FROM isolation_test ORDER BY id;

-- Main branch CRUD operations
\c - - - :main_port
SET client_min_messages = WARNING;

INSERT INTO isolation_test (category, value, description) VALUES
    ('main_insert', 100, 'Main insert 1'),
    ('main_insert', 101, 'Main insert 2');

UPDATE isolation_test 
SET value = value + 1000, description = description || ' [MAIN UPDATE]'
WHERE category = 'initial' AND id <= 2;

DELETE FROM isolation_test WHERE id = 3;

SELECT 'MAIN - After CRUD:' AS info;
SELECT * FROM isolation_test ORDER BY id;

-- Verify branch NOT affected by main CRUD
\c - - - :branch1_port
SET client_min_messages = WARNING;

SELECT 'BRANCH - After main CRUD (should be unchanged):' AS info;
SELECT * FROM isolation_test ORDER BY id;

SELECT 'BRANCH - Check main_insert (should be 0):' AS info;
SELECT COUNT(*) FROM isolation_test WHERE category = 'main_insert';

SELECT 'BRANCH - Check id=3 exists (should be 1):' AS info;
SELECT COUNT(*) FROM isolation_test WHERE id = 3;

-- Branch CRUD operations
INSERT INTO isolation_test (category, value, description) VALUES
    ('branch_insert', 200, 'Branch insert 1'),
    ('branch_insert', 201, 'Branch insert 2');

UPDATE isolation_test 
SET value = value + 2000, description = description || ' [BRANCH UPDATE]'
WHERE category = 'initial' AND id IN (4, 5);

DELETE FROM isolation_test WHERE id = 1;

SELECT 'BRANCH - After CRUD:' AS info;
SELECT * FROM isolation_test ORDER BY id;

-- Verify main NOT affected by branch CRUD
\c - - - :main_port
SET client_min_messages = WARNING;

SELECT 'MAIN - After branch CRUD (should be unchanged):' AS info;
SELECT * FROM isolation_test ORDER BY id;

SELECT 'MAIN - Check branch_insert (should be 0):' AS info;
SELECT COUNT(*) FROM isolation_test WHERE category = 'branch_insert';

SELECT 'MAIN - Check id=1 exists (should be 1):' AS info;
SELECT COUNT(*) FROM isolation_test WHERE id = 1;

-- Final state comparison
SELECT 'MAIN - Final summary:' AS info;
SELECT category, COUNT(*) AS count FROM isolation_test GROUP BY category ORDER BY category;

\c - - - :branch1_port
SET client_min_messages = WARNING;
SELECT 'BRANCH - Final summary:' AS info;
SELECT category, COUNT(*) AS count FROM isolation_test GROUP BY category ORDER BY category;

-- Cleanup: Ensure on main branch before stopping branch endpoint
\c - - - :main_port
SET client_min_messages = WARNING;
DROP TABLE IF EXISTS isolation_test CASCADE;
\! neon_local endpoint stop ep-isolation-test --mode immediate --destroy 2>&1 || true

