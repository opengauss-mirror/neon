-- Neon Branch Creation with Specified LSN Test
-- Tests branch creation at specific LSN with data inheritance and isolation

\c - - - :main_port
SET client_min_messages = WARNING;

DROP TABLE IF EXISTS lsn_test CASCADE;

CREATE TABLE lsn_test (
    id SERIAL PRIMARY KEY,
    stage TEXT NOT NULL,
    value INTEGER NOT NULL,
    description TEXT
);

-- Insert stage 1 data
INSERT INTO lsn_test (stage, value, description) VALUES
    ('stage1', 1, 'Data 1'),
    ('stage1', 2, 'Data 2'),
    ('stage1', 3, 'Data 3');

SELECT 'MAIN - Stage 1 data:' AS info;
SELECT * FROM lsn_test ORDER BY id;

-- Save current LSN to file for branch creation
\t on
\o /tmp/branch_lsn.txt
SELECT pg_current_xlog_location();
\o
\t off

-- Insert stage 2 data (after LSN is recorded)
INSERT INTO lsn_test (stage, value, description) VALUES
    ('stage2', 4, 'Data 4'),
    ('stage2', 5, 'Data 5');

SELECT 'MAIN - After stage 2 (branch should NOT have this):' AS info;
SELECT * FROM lsn_test ORDER BY id;

-- Create branch at stage 1 LSN
\! neon_local timeline branch --branch-name lsn_test_branch --ancestor-start-lsn $(cat /tmp/branch_lsn.txt | tr -d ' \n\t')
\! neon_local endpoint create ep-lsn-test --branch-name lsn_test_branch --pg-port ${BRANCH1_PORT}
\! neon_local endpoint start ep-lsn-test
\! sleep 5

-- Verify branch only has stage 1 data
\c - - - :branch1_port
SET client_min_messages = WARNING;

SELECT 'BRANCH - Inherited data (should only have stage1):' AS info;
SELECT * FROM lsn_test ORDER BY id;

SELECT 'BRANCH - Check stage2 (should be 0):' AS info;
SELECT COUNT(*) FROM lsn_test WHERE stage = 'stage2';

SELECT 'BRANCH - Stage summary (should only show stage1):' AS info;
SELECT stage, COUNT(*) FROM lsn_test GROUP BY stage ORDER BY stage;

-- Branch CRUD operations
INSERT INTO lsn_test (stage, value, description) VALUES
    ('branch_data', 100, 'Branch insert 1'),
    ('branch_data', 101, 'Branch insert 2');

UPDATE lsn_test 
SET value = value + 1000, description = description || ' [BRANCH]'
WHERE stage = 'stage1' AND id <= 2;

DELETE FROM lsn_test WHERE id = 3;

SELECT 'BRANCH - After CRUD:' AS info;
SELECT * FROM lsn_test ORDER BY id;

-- Verify main not affected by branch CRUD
\c - - - :main_port
SET client_min_messages = WARNING;

SELECT 'MAIN - After branch CRUD (should be unchanged):' AS info;
SELECT * FROM lsn_test ORDER BY id;

SELECT 'MAIN - Check branch_data (should be 0):' AS info;
SELECT COUNT(*) FROM lsn_test WHERE stage = 'branch_data';

SELECT 'MAIN - Check id=3 (should be 1):' AS info;
SELECT COUNT(*) FROM lsn_test WHERE id = 3;

-- Main additional CRUD operations
INSERT INTO lsn_test (stage, value, description) VALUES
    ('stage3', 6, 'Data 6');

UPDATE lsn_test 
SET value = value + 2000
WHERE stage = 'stage2';

DELETE FROM lsn_test WHERE id = 1;

SELECT 'MAIN - After additional CRUD:' AS info;
SELECT * FROM lsn_test ORDER BY id;

-- Verify branch not affected by main operations
\c - - - :branch1_port
SET client_min_messages = WARNING;

SELECT 'BRANCH - After main operations (should be unchanged):' AS info;
SELECT * FROM lsn_test ORDER BY id;

SELECT 'BRANCH - Check stage3 (should be 0):' AS info;
SELECT COUNT(*) FROM lsn_test WHERE stage = 'stage3';

-- Final summary
SELECT 'BRANCH - Final summary:' AS info;
SELECT stage, COUNT(*) AS count FROM lsn_test GROUP BY stage ORDER BY stage;

\c - - - :main_port
SET client_min_messages = WARNING;

SELECT 'MAIN - Final summary:' AS info;
SELECT stage, COUNT(*) AS count FROM lsn_test GROUP BY stage ORDER BY stage;

-- Cleanup
DROP TABLE IF EXISTS lsn_test CASCADE;
\c - - - :main_port
SET client_min_messages = WARNING;
\! neon_local endpoint stop ep-lsn-test --mode immediate --destroy 2>&1 || true
\! rm -f /tmp/branch_lsn.txt

