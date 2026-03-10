-- ============================================================================
-- Neon Test Environment Setup
-- ============================================================================
-- This script initializes the test environment for Neon regression tests.
-- It MUST be run first before any other test cases.
--
-- Actions:
--   1. Display connection and version info
--   2. Create test schema for isolation
--   3. Create test results tracking table
--   4. Verify basic connectivity
-- ============================================================================

\echo '=============================================='
\echo '  Neon Test Environment Initialization'
\echo '=============================================='

-- Display current connection info
\echo '--- Connection Info ---'
\conninfo

-- Show PostgreSQL/openGauss version
\echo '--- Database Version ---'
SELECT version();

-- Show current database and user
SELECT current_database() AS database, current_user AS user;

-- ============================================================================
-- Create Test Schema
-- ============================================================================
\echo '--- Creating Test Schema ---'

-- Drop existing test schema if present (clean start)
DROP SCHEMA IF EXISTS neon_test CASCADE;

-- Create fresh test schema
CREATE SCHEMA neon_test;

-- Set search path for subsequent tests
SET search_path TO neon_test, public;

-- Verify schema was created
SELECT nspname AS schema_name 
FROM pg_namespace 
WHERE nspname = 'neon_test';

-- ============================================================================
-- Create Test Results Tracking Table
-- ============================================================================
\echo '--- Creating Test Results Table ---'

CREATE TABLE neon_test.test_results (
    id SERIAL PRIMARY KEY,
    test_name VARCHAR(100) NOT NULL,
    test_phase VARCHAR(50) NOT NULL,
    test_status VARCHAR(20) NOT NULL,
    message TEXT,
    start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    end_time TIMESTAMP,
    duration_ms INT
);

-- Create index for fast lookups
CREATE INDEX idx_test_results_status ON neon_test.test_results(test_status);
CREATE INDEX idx_test_results_phase ON neon_test.test_results(test_phase);

-- Insert setup start record
INSERT INTO neon_test.test_results (test_name, test_phase, test_status, message)
VALUES ('neon_setup', 'initialization', 'RUNNING', 'Starting environment setup');

-- ============================================================================
-- Create Environment Info Table
-- ============================================================================
\echo '--- Recording Environment Info ---'

CREATE TABLE neon_test.env_info (
    key VARCHAR(50) PRIMARY KEY,
    value TEXT,
    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Record environment information
INSERT INTO neon_test.env_info (key, value) VALUES
    ('database', current_database()),
    ('user', current_user),
    ('pg_version', version()),
    ('init_time', CURRENT_TIMESTAMP::TEXT);

SELECT key, value FROM neon_test.env_info ORDER BY key;

-- ============================================================================
-- Verify Basic Operations
-- ============================================================================
\echo '--- Verifying Basic Operations ---'

-- Test table creation
CREATE TABLE neon_test.verify_ops (
    id INT PRIMARY KEY,
    data VARCHAR(50)
);

-- Test insert
INSERT INTO neon_test.verify_ops VALUES (1, 'test_insert');

-- Test select
SELECT * FROM neon_test.verify_ops;

-- Test update
UPDATE neon_test.verify_ops SET data = 'test_update' WHERE id = 1;

-- Test delete
DELETE FROM neon_test.verify_ops WHERE id = 1;

-- Cleanup verification table
DROP TABLE neon_test.verify_ops;


-- ============================================================================
-- Update Setup Status
-- ============================================================================

UPDATE neon_test.test_results 
SET test_status = 'PASSED',
    end_time = CURRENT_TIMESTAMP,
    duration_ms = EXTRACT(MILLISECONDS FROM (CURRENT_TIMESTAMP - start_time))::INT,
    message = 'Environment setup completed successfully'
WHERE test_name = 'neon_setup';

-- Show setup result
SELECT test_name, test_status, message 
FROM neon_test.test_results 
WHERE test_name = 'neon_setup';

\echo '=============================================='
\echo '  Environment Initialization Complete'
\echo '=============================================='

