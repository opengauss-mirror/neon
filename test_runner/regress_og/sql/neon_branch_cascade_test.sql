-- ============================================================================
-- STEP 1: Create test data on MAIN branch
-- ============================================================================
DROP TABLE IF EXISTS test_branch;

CREATE TABLE test_branch (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    value INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO test_branch (name, value) VALUES 
    ('item1', 100),
    ('item2', 200),
    ('item3', 300);

SELECT * FROM test_branch;

SELECT COUNT(*) FROM test_branch;

CREATE TABLE large_table (
    id SERIAL PRIMARY KEY,
    data TEXT,
    created_at TIMESTAMP DEFAULT now()
);

INSERT INTO large_table (data) 
SELECT md5(random()::text) FROM generate_series(1, 100000);
SELECT COUNT(*) FROM large_table;

-- ============================================================================
-- STEP 2: Create a new branch using shell command
-- ============================================================================
-- Create the branch using full path (neon_root is set by the wrapper)
\! neon_local timeline branch --branch-name branch1

-- Create and start endpoint for the branch
-- Use $BRANCH1_PORT environment variable (exported by wrapper) to match :branch1_port
\! neon_local endpoint create ep_branch1 --branch-name branch1 --pg-port ${BRANCH1_PORT}

\! neon_local endpoint start ep_branch1

-- ============================================================================
-- STEP 3: Switch to new branch and check main branch data
-- ============================================================================
-- Switch to branch endpoint (port 55435 = branch1_port)
\c - - - :branch1_port

SELECT * FROM test_branch;

SELECT COUNT(*) FROM large_table;

UPDATE large_table SET data = 'modified' WHERE id <= 10000;

SELECT COUNT(*) FROM large_table WHERE data = 'modified';

-- do some ddl in old table
ALTER TABLE test_branch ADD COLUMN new_int_col INT;

ALTER TABLE test_branch DROP COLUMN created_at;

create index on test_branch(name);

create index on test_branch(value);

\d+ test_branch;

-- do some dml
delete from test_branch where id = 300;

INSERT INTO test_branch (name, value) VALUES ('branch1_data', 999);

update test_branch set name = 'branch_data_name';

SELECT * FROM test_branch;

-- create new table in branch
CREATE TABLE branch_only_table (id INT PRIMARY KEY, data TEXT);
INSERT INTO branch_only_table VALUES (1, 'only on branch1');
INSERT INTO branch_only_table VALUES (2, 'only on branch1');
INSERT INTO branch_only_table VALUES (3, 'only on branch1');
INSERT INTO branch_only_table VALUES (4, 'only on branch1');
INSERT INTO branch_only_table VALUES (5, 'only on branch1');

SELECT * FROM branch_only_table;

\c - - - :main_port

-- branch operation dont affect main branch
-- expect 0, no data modified
SELECT COUNT(*) FROM large_table WHERE data = 'modified';

\d+ test_branch;
SELECT * FROM test_branch;

UPDATE test_branch SET value = 111 WHERE name = 'item1';

SELECT * FROM test_branch WHERE name = 'item1';

CREATE TABLE table_only_in_main (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    value INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO table_only_in_main (name, value) VALUES 
    ('item1', 100),
    ('item2', 200),
    ('item3', 300);

\c - - - :branch1_port
-- main endpoint update dont affect branch data
SELECT value FROM test_branch WHERE name = 'item1';

-- expect error
\d+ table_only_in_main;

BEGIN;
INSERT INTO test_branch (name, value) VALUES ('tx_test', 500);
SAVEPOINT sp1;
INSERT INTO test_branch (name, value) VALUES ('tx_test2', 600);
ROLLBACK TO sp1;
COMMIT;

-- expect only tx_test inserted
SELECT * FROM test_branch WHERE name LIKE 'tx%';

-- Stop branch endpoint
\! neon_local endpoint stop ep_branch1

\! sleep 5

\! neon_local endpoint start ep_branch1

\c - - - :branch1_port

-- OK after restart
SELECT * FROM test_branch;

SELECT * FROM branch_only_table;

-- ============================================================================
-- STEP 4: Cascading Branch
-- ============================================================================
-- Create the branch using full path (neon_root is set by the wrapper)
\! neon_local timeline branch --branch-name branch2 --ancestor-branch-name branch1

\! neon_local endpoint create ep_branch2 --branch-name branch2 --pg-port ${BRANCH2_PORT}

\! neon_local endpoint start ep_branch2

\c - - - :branch2_port
-- data same as branch1
SELECT * FROM test_branch;

SELECT * FROM branch_only_table;

-- create new table in branch
CREATE TABLE branch2_only_table (id INT PRIMARY KEY, data TEXT);
INSERT INTO branch2_only_table VALUES (1, 'only on branch2');
INSERT INTO branch2_only_table VALUES (2, 'only on branch2');
INSERT INTO branch2_only_table VALUES (3, 'only on branch2');
INSERT INTO branch2_only_table VALUES (4, 'only on branch2');
INSERT INTO branch2_only_table VALUES (5, 'only on branch2');

select * from branch2_only_table;

drop table test_branch;

drop table branch_only_table;

-- Create the branch using full path (neon_root is set by the wrapper)
\! neon_local timeline branch --branch-name branch3 --ancestor-branch-name branch2

\! neon_local endpoint create ep_branch3 --branch-name branch3 --pg-port ${BRANCH3_PORT}

\! neon_local endpoint start ep_branch3

\c - - - :branch3_port
-- error table drop in branch2
SELECT * FROM test_branch;
-- error table drop in branch2
SELECT * FROM branch_only_table;

-- create new table in branch
CREATE TABLE branch3_only_table (id INT PRIMARY KEY, data TEXT);
INSERT INTO branch3_only_table VALUES (1, 'only on branch3');
INSERT INTO branch3_only_table VALUES (2, 'only on branch3');
INSERT INTO branch3_only_table VALUES (3, 'only on branch3');
INSERT INTO branch3_only_table VALUES (4, 'only on branch3');
INSERT INTO branch3_only_table VALUES (5, 'only on branch3');
select * from branch3_only_table;

-- expect error
\c - - - :branch2_port
\d+ branch3_only_table;

-- expect error
\c - - - :branch1_port
\d+ branch3_only_table;

\! neon_local endpoint stop ep_branch1;

\! neon_local endpoint stop ep_branch2;

\! neon_local endpoint stop ep_branch3;
