-- Neon Branch Data Inheritance Test
-- Tests that data from main branch is inherited by new branches

\c - - - :main_port
SET client_min_messages = WARNING;

DROP TABLE IF EXISTS inherit_test CASCADE;

CREATE TABLE inherit_test (
    id SERIAL PRIMARY KEY,
    source_branch TEXT NOT NULL,
    data_value INT NOT NULL,
    message TEXT
);

-- Insert test data on main
INSERT INTO inherit_test (source_branch, data_value, message) VALUES
    ('main', 100, 'First row created on main'),
    ('main', 200, 'Second row created on main'),
    ('main', 300, 'Third row created on main');

CREATE INDEX idx_inherit_test_value ON inherit_test(data_value);

SELECT 'MAIN - Initial data:' AS info;
SELECT * FROM inherit_test ORDER BY id;
SELECT COUNT(*) AS row_count FROM inherit_test;

-- Create branch
\! neon_local timeline branch --branch-name test_inherit_branch
\! neon_local endpoint create ep-test_inherit --branch-name test_inherit_branch --pg-port ${BRANCH1_PORT}
\! neon_local endpoint start ep-test_inherit
\! sleep 5

-- Verify inherited data on branch
\c - - - :branch1_port
SET client_min_messages = WARNING;

SELECT 'BRANCH - Inherited data:' AS info;
SELECT * FROM inherit_test ORDER BY id;
SELECT COUNT(*) AS row_count FROM inherit_test;

-- Check if index was inherited
SELECT indexname FROM pg_indexes WHERE tablename = 'inherit_test' AND indexname = 'idx_inherit_test_value';

-- Add branch-specific data
INSERT INTO inherit_test (source_branch, data_value, message) VALUES
    ('branch', 1000, 'First row created on branch'),
    ('branch', 2000, 'Second row created on branch');

SELECT 'BRANCH - After insert:' AS info;
SELECT * FROM inherit_test ORDER BY id;
SELECT source_branch, COUNT(*) AS rows FROM inherit_test GROUP BY source_branch ORDER BY source_branch;

-- Verify isolation on main
\c - - - :main_port
SET client_min_messages = WARNING;

SELECT 'MAIN - After branch insert (should be unchanged):' AS info;
SELECT * FROM inherit_test ORDER BY id;
SELECT COUNT(*) AS row_count FROM inherit_test;
SELECT COUNT(*) AS branch_data_count FROM inherit_test WHERE source_branch = 'branch';

-- Add more data on main after branch was created
INSERT INTO inherit_test (source_branch, data_value, message) VALUES
    ('main_after_branch', 400, 'Added after branch'),
    ('main_after_branch', 500, 'Should NOT appear on branch');

SELECT 'MAIN - After adding post-branch data:' AS info;
SELECT * FROM inherit_test ORDER BY id;

-- Verify branch does NOT have post-branch main data
\c - - - :branch1_port
SET client_min_messages = WARNING;

SELECT 'BRANCH - After main insert (should be unchanged):' AS info;
SELECT * FROM inherit_test ORDER BY id;
SELECT COUNT(*) AS post_branch_count FROM inherit_test WHERE source_branch = 'main_after_branch';

-- Final summary
SELECT 'BRANCH - Final summary:' AS info;
SELECT source_branch, COUNT(*) AS rows FROM inherit_test GROUP BY source_branch ORDER BY source_branch;

\c - - - :main_port
SET client_min_messages = WARNING;

SELECT 'MAIN - Final summary:' AS info;
SELECT source_branch, COUNT(*) AS rows FROM inherit_test GROUP BY source_branch ORDER BY source_branch;

-- Cleanup
DROP TABLE IF EXISTS inherit_test CASCADE;
\! neon_local endpoint stop ep-test_inherit --mode immediate --destroy 2>&1 || true
