-- ============================================================================
-- Neon Test Environment Cleanup
-- ============================================================================
-- This script cleans up the test environment after all tests complete.
-- It MUST be run last after all other test cases.
--
-- Actions:
--   1. Display test summary and results
--   2. Check for any failed tests
--   3. Drop test schema and all objects
--   4. Verify cleanup was successful
-- ============================================================================

\echo '=============================================='
\echo '  Neon Test Environment Cleanup'
\echo '=============================================='

SET search_path TO neon_test, public;

-- ============================================================================
-- Display Test Summary
-- ============================================================================
\echo '--- Test Summary by Phase ---'

SELECT * FROM neon_test.get_test_summary();

\echo '--- All Test Results ---'

SELECT 
    test_name,
    test_phase,
    test_status,
    duration_ms || ' ms' AS duration,
    message
FROM neon_test.test_results
ORDER BY id;

-- ============================================================================
-- Check for Failed Tests
-- ============================================================================
\echo '--- Checking for Failed Tests ---'

DO $$
DECLARE
    v_failed_count INT;
    v_passed_count INT;
    v_total_count INT;
BEGIN
    SELECT 
        COUNT(*) FILTER (WHERE test_status = 'FAILED'),
        COUNT(*) FILTER (WHERE test_status = 'PASSED'),
        COUNT(*)
    INTO v_failed_count, v_passed_count, v_total_count
    FROM neon_test.test_results;
    
    RAISE NOTICE '========================================';
    RAISE NOTICE 'FINAL TEST RESULTS';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Total Tests: %', v_total_count;
    RAISE NOTICE 'Passed: %', v_passed_count;
    RAISE NOTICE 'Failed: %', v_failed_count;
    RAISE NOTICE '========================================';
    
    IF v_failed_count > 0 THEN
        RAISE WARNING 'There are % failed tests!', v_failed_count;
    ELSE
        RAISE NOTICE 'All tests passed successfully!';
    END IF;
END $$;

-- Show failed tests details if any
SELECT test_name, test_phase, message
FROM neon_test.test_results
WHERE test_status = 'FAILED'
ORDER BY id;

-- ============================================================================
-- Save Final Statistics
-- ============================================================================
\echo '--- Recording Cleanup Phase ---'

-- Record cleanup start
INSERT INTO neon_test.test_results (test_name, test_phase, test_status, message)
VALUES ('neon_cleanup', 'cleanup', 'RUNNING', 'Starting environment cleanup');

-- ============================================================================
-- Check for Orphaned Objects
-- ============================================================================
\echo '--- Checking for Orphaned Objects ---'

-- List any remaining tables (besides our tracking tables)
SELECT tablename 
FROM pg_tables 
WHERE schemaname = 'neon_test' 
  AND tablename NOT IN ('test_results', 'env_info')
ORDER BY tablename;

-- List any remaining functions
SELECT proname AS function_name
FROM pg_proc p
JOIN pg_namespace n ON p.pronamespace = n.oid
WHERE n.nspname = 'neon_test'
  AND proname NOT LIKE 'record_%'
  AND proname NOT LIKE 'assert_%'
  AND proname NOT LIKE 'get_%'
  AND proname NOT LIKE 'table_%'
  AND proname NOT LIKE 'index_%'
ORDER BY proname;

-- ============================================================================
-- Display Environment Info
-- ============================================================================
\echo '--- Environment Info ---'

SELECT key, value FROM neon_test.env_info ORDER BY key;

-- ============================================================================
-- Update Cleanup Status
-- ============================================================================

UPDATE neon_test.test_results 
SET test_status = 'PASSED',
    end_time = CURRENT_TIMESTAMP,
    duration_ms = EXTRACT(MILLISECONDS FROM (CURRENT_TIMESTAMP - start_time))::INT,
    message = 'Cleanup completed successfully'
WHERE test_name = 'neon_cleanup';

-- Final summary
\echo '--- Final Test Results Summary ---'

SELECT 
    test_status,
    COUNT(*) AS count
FROM neon_test.test_results
GROUP BY test_status
ORDER BY test_status;

-- ============================================================================
-- Drop Test Schema
-- ============================================================================
\echo '--- Dropping Test Schema ---'

-- This will drop all objects in the schema
DROP SCHEMA IF EXISTS neon_test CASCADE;

-- ============================================================================
-- Verify Cleanup
-- ============================================================================
\echo '--- Verifying Cleanup ---'

-- Check if schema still exists
SELECT 
    CASE 
        WHEN EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = 'neon_test')
        THEN 'FAILED: Schema still exists!'
        ELSE 'SUCCESS: Schema removed successfully'
    END AS cleanup_verification;

-- Reset search path
SET search_path TO public;

\echo '=============================================='
\echo '  Environment Cleanup Complete'
\echo '=============================================='

