# Rules to normalize test outputs for Neon + openGauss
#
# This file contains sed substitution rules that normalize test output
# before comparing with expected results. This helps handle non-deterministic
# elements like timestamps, PIDs, ports, LSNs, etc.

# ============================================================================
# Neon-specific normalizations
# ============================================================================

# Normalize WAL LSN values (format: X/XXXXXXXX)
s/[0-9A-Fa-f]+\/[0-9A-Fa-f]+/X\/XXXXXXXX/g

# Normalize timeline IDs (32 hex chars) - must come before tenant IDs
# Match various formats: timeline 'xxx', timeline "xxx", timeline xxx, or standalone xxx
s/timeline '[0-9a-f]{32}'/timeline 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'/g
s/timeline "[0-9a-f]{32}"/timeline "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"/g
s/timeline [0-9a-f]{32}/timeline xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx/g

# Normalize tenant IDs (32 hex chars) - general catch-all for any remaining 32-char hex strings
s/[0-9a-f]{32}/xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx/g

# Normalize shard IDs
s/shard [0-9]+/shard xxxxx/g
s/Shard [0-9]+/Shard xxxxx/g

# Normalize placement IDs
s/placement [0-9]+/placement xxxxx/g

# Normalize page LSN in debug messages
s/page_lsn=[0-9A-Fa-f\/]+/page_lsn=X\/X/g

# Normalize block numbers in debug messages
s/blkno=[0-9]+/blkno=XXX/g

# ============================================================================
# openGauss-specific normalizations
# ============================================================================

# Normalize entire openGauss version string
# Matches the complete version info: (openGauss ...) compiled at ... -bit
# This makes tests independent of version, build, compile time, commit, mr, architecture, etc.
s/\(openGauss [^)]+\).*-bit/(openGauss VERSION)/g

# Fallback: Normalize openGauss version string compile time (if above doesn't match)
# Format: "compiled at YYYY-MM-DD HH:MM:SS" -> "compiled at YYYY-MM-DD XX:XX:XX"
s/compiled at [0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}/compiled at YYYY-MM-DD XX:XX:XX/g

# Fallback: Normalize openGauss build hash (if above doesn't match)
# Format: "build 423b122f" -> "build XXXXXXXX"
s/build [0-9a-f]{8}/build XXXXXXXX/g

# Remove NEON-SLRU debug messages
/\[NEON-SLRU\]/d

# Remove LAYERDBG debug messages
/\[LAYERDBG\]/d

# Remove openGauss WARNING messages with file paths
/gsql:.*WARNING:/d

# Note: NOTICE messages are kept but paths are normalized by rules below
# This provides useful debugging information while maintaining environment independence
# /gsql:.*NOTICE:/d

# Remove gsql tuples-only mode toggle messages
/^Showing only tuples\.$/d
/^Tuples only is off\.$/d

# Remove DETAIL messages from CASCADE drops (non-deterministic)
/^DETAIL:.*drop cascades to/d

# Remove total time output
/^total time:/d

# Remove progress dots from neon_local commands
/^\.+$/d

# Remove neon_local endpoint output (both success and error cases)
# This includes start/stop operations that may fail intermittently
# Success: "compute_ctl stopped", "null", "Starting existing endpoint..."
# Error: "command failed...", "SIGKILL", "Caused by:", connection errors, etc.
/^compute_ctl stopped$/d
/^null$/d
/^SIGKILL & wait the started process$/d
/^command failed: pg_ctl failed/d
/^command failed: endpoint .* not found$/d
/^command failed: timed out .* waiting to connect/d
/^Caused by:$/d
/^    [0-9]+: /d
/^\[.*\]\[.*\]\[\]\[gs_ctl\]:/d
/^waiting for server to shut down/d
/^, stderr:$/d

# Normalize endpoint destroy path output
# Format: Destroying postgres data directory '/path/to/neon_branch_dev/...'
# Replace the full path with a normalized version
s|Destroying postgres data directory '[^']*neon_branch_dev/test_runner/regress_og/\.neon/endpoints/([^']*)'|Destroying postgres data directory 'NEON_ROOT/.neon/endpoints/\1'|g

# Normalize openGauss error line numbers
/^LINE [0-9]+:/d
/^ *\^$/d

# Remove openGauss context lines that contain line numbers
# Note: This rule is commented out because it requires extended regex which is not portable
# If needed, use: sed -E or sed -r
# s/(CONTEXT:.*line )[0-9]+/\1XX/g

# Remove PL/pgSQL function context traces (non-deterministic output)
/^CONTEXT:  referenced column:/d
/^SQL statement "SELECT/d
/^        WHERE schemaname/d
/^PL\/pgSQL function .* line [0-9]+ at/d
/^referenced column:/d

# Note: Removed problematic rule that was matching all pipe characters
# Original intent was to normalize table cells ending with numbers
# But it was causing all table output to be corrupted
# If needed, use a more specific pattern that matches the full line context

# ============================================================================
# Port and connection normalizations
# ============================================================================

# Normalize localhost ports
s/localhost:[0-9]+/localhost:xxxxx/g
s/127\.0\.0\.1:[0-9]+/127.0.0.1:xxxxx/g

# Normalize port numbers in connection strings
s/port=[0-9]+/port=xxxxx/g
s/port [0-9]+/port xxxxx/g
s/at port "[0-9]+"/at port "xxxxx"/g
s/port :[0-9]+/port :xxxxx/g

# Normalize specific port numbers used in tests (55432, 55435, 55436, 55437)
s/\b55432\b/xxxxx/g
s/\b55435\b/xxxxx/g
s/\b55436\b/xxxxx/g
s/\b55437\b/xxxxx/g

# Normalize connection IDs
s/connectionId: [0-9]+/connectionId: xxxxxxx/g

# ============================================================================
# OID and system catalog normalizations
# ============================================================================

# Normalize OIDs
s/OID [0-9]+/OID xxxxx/g
s/oid=[0-9]+/oid=xxxxx/g

# Normalize relation OIDs in format rel=X/Y/Z
s/rel=[0-9]+\/[0-9]+\/[0-9]+/rel=X\/X\/X/g

# Normalize sequence values
s/nextval\('[^']+'\)=[0-9]+/nextval('xxx')=XXX/g

# Normalize toast table names
s/pg_toast_[0-9]+/pg_toast_xxxxx/g

# ============================================================================
# Transaction and process normalizations
# ============================================================================

# Normalize transaction IDs
s/transaction [0-9]+/transaction xxxxx/g
s/xid [0-9]+/xid xxxxx/g
s/xid=[0-9]+/xid=xxxxx/g

# Normalize process IDs
s/pid=[0-9]+/pid=xxxxx/g
s/PID [0-9]+/PID xxxxx/g
s/pid [0-9]+/pid xxxxx/g

# Normalize backend PIDs in log messages
s/\[pid=[0-9]+\]/[pid=xxxxx]/g

# ============================================================================
# Timestamp normalizations
# ============================================================================

# Normalize timestamps in ISO format
# Format: YYYY-MM-DD HH:MM:SS.microseconds+timezone
s/[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]+\+[0-9]{2}/YYYY-MM-DD HH:MM:SS.XXX+XX/g
s/[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]+/YYYY-MM-DD HH:MM:SS.XXX/g
s/[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}/YYYY-MM-DDTHH:MM:SS/g

# Normalize duration/timing values
s/duration: [0-9]+\.[0-9]+ ms/duration: X.XXX ms/g
s/Time: [0-9]+\.[0-9]+ ms/Time: X.XXX ms/g
s/[0-9]+ ms/XXX ms/g

# ============================================================================
# Memory and size normalizations
# ============================================================================

# Normalize memory sizes
s/[0-9]+ bytes/XXX bytes/g
s/[0-9]+ kB/XXX kB/g
s/[0-9]+ MB/XXX MB/g
s/[0-9]+ GB/XXX GB/g

# ============================================================================
# Table formatting normalizations
# ============================================================================

# Normalize table separator lines (variable width)
s/^-[-+]+$/---------------------------------------------------------------------/g
s/^\+[-+]+\+$/+---------------------------------------------------------------------+/g

# Remove trailing whitespace
s/ *$//g

# ============================================================================
# EXPLAIN output normalizations
# ============================================================================

# Normalize cost values in EXPLAIN
s/cost=[0-9]+\.[0-9]+\.\.[0-9]+\.[0-9]+/cost=X.XX..X.XX/g
s/rows=[0-9]+/rows=XXX/g
s/width=[0-9]+/width=XXX/g

# Normalize actual time in EXPLAIN ANALYZE
s/actual time=[0-9]+\.[0-9]+\.\.[0-9]+\.[0-9]+/actual time=X.XX..X.XX/g
s/loops=[0-9]+/loops=XXX/g

# ============================================================================
# Path normalizations
# ============================================================================

# Normalize project root path in gsql messages (NOTICE, ERROR, WARNING, etc.)
# This handles absolute paths that vary across different environments
# Format: gsql:/any/path/to/neon_branch_dev/test_runner/regress_og/sql/... 
#      -> gsql:NEON_ROOT/sql/...
# This makes tests independent of where the neon repository is located
s|gsql:[^:]*neon_branch_dev/test_runner/regress_og/sql/|gsql:NEON_ROOT/sql/|g
s|gsql:[^:]*neon_branch_dev/|gsql:NEON_ROOT/|g

# Normalize file paths containing test output directories
s|/home/[^/]+/[^/]+/neon_branch_dev/test_output/[^/]+|/TEST_OUTPUT_DIR|g

# Normalize temporary file paths
s|/tmp/[^ ]+|/tmp/xxxxx|g

# ============================================================================
# Miscellaneous normalizations
# ============================================================================

# Normalize random number sequences
s/seed=[0-9]+/seed=xxxxx/g

# Normalize hash values
s/hash=[0-9a-f]+/hash=xxxxx/g

# Remove empty lines that might differ
/^$/d

