#!/bin/bash
#
# Neon Regression Test Runner for openGauss
#
# This script runs SQL regression tests and compares output with expected results.
# It follows the pattern used by PostgreSQL/Citus pg_regress.
#
# Like Citus's pg_regress_multi.pl, this script dynamically generates a gsql
# wrapper that includes preset port variables for multi-endpoint testing.
#
# Usage:
#   ./run_tests.sh --schedule=<schedule_file> [options]
#   ./run_tests.sh --test=<test_name> [options]
#
# Options:
#   --schedule=FILE     Run all tests in schedule file
#   --test=NAME         Run a specific test
#   --host=HOST         Database host (default: 127.0.0.1)
#   --port=PORT         Database port (default: 55432)
#   --user=USER         Database user (default: cloud_admin)
#   --dbname=DB         Database name (default: postgres)
#   --keep-results      Don't clean results directory before run
#   --verbose           Show verbose output
#   --help              Show this help message

# Note: We don't use 'set -e' here because we want to continue running tests
# even if some fail, and handle errors explicitly.

# ============================================================================
# Configuration
# ============================================================================

# Use realpath to resolve symlinks (important for .neon directory detection)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
NEON_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd -P)"

# Directories
SQL_DIR="${SCRIPT_DIR}/sql"
EXPECTED_DIR="${SCRIPT_DIR}/expected"
RESULTS_DIR="${SCRIPT_DIR}/results"
BIN_DIR="${SCRIPT_DIR}/bin"
TMP_BINDIR="${SCRIPT_DIR}/tmp-bin"  # Dynamic wrapper directory (like Citus)

# Default connection settings
HOST="127.0.0.1"
PORT="55432"
USER="cloud_admin"
DBNAME="postgres"

# Default options
SCHEDULE=""
TEST=""
EXTRA_TESTS=""
KEEP_RESULTS=false
VERBOSE=false

# Branch/endpoint support
BRANCH_NAME=""
ENDPOINT_PORT=""

# Port settings for multi-endpoint testing
# Main endpoint uses 55432, branches start from 55435
MAIN_PORT="55432"
BRANCH1_PORT="55435"
BRANCH2_PORT="55438"
BRANCH3_PORT="55441"

# Version
# PG_VERSION: PostgreSQL major version for neon_local (14 maps to V702 internally)
# OG_VERSION: openGauss directory version (e.g., V702)
PG_VERSION="${DEFAULT_PG_VERSION:-14}"
OG_VERSION="${DEFAULT_OG_VERSION:-V702}"
OG_INSTALL="${OPENGAUSS_DISTRIB_DIR:-${NEON_ROOT}/og_install}"
GSQL_REAL="${OG_INSTALL}/${OG_VERSION}/bin/gsql"

export LD_LIBRARY_PATH="${OG_INSTALL}/${OG_VERSION}/lib:${LD_LIBRARY_PATH:-}"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# ============================================================================
# Functions
# ============================================================================

usage() {
    head -30 "$0" | grep -E "^#" | sed 's/^# //' | sed 's/^#//'
    exit 0
}

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_pass() {
    echo -e "${GREEN}[PASS]${NC} $1"
}

log_fail() {
    echo -e "${RED}[FAIL]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

# Parse command line arguments
parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --schedule=*)
                SCHEDULE="${1#*=}"
                shift
                ;;
            --test=*)
                TEST="${1#*=}"
                shift
                ;;
            --host=*)
                HOST="${1#*=}"
                shift
                ;;
            --port=*)
                PORT="${1#*=}"
                MAIN_PORT="${PORT}"
                shift
                ;;
            --user=*)
                USER="${1#*=}"
                shift
                ;;
            --dbname=*)
                DBNAME="${1#*=}"
                shift
                ;;
            --extra-tests=*)
                EXTRA_TESTS="${1#*=}"
                shift
                ;;
            --skip-env-setup)
                SKIP_ENV_SETUP=1
                shift
                ;;
            --branch=*)
                BRANCH_NAME="${1#*=}"
                shift
                ;;
            --endpoint-port=*)
                ENDPOINT_PORT="${1#*=}"
                PORT="${ENDPOINT_PORT}"
                shift
                ;;
            --keep-results)
                KEEP_RESULTS=true
                shift
                ;;
            --verbose)
                VERBOSE=true
                shift
                ;;
            --help|-h)
                usage
                ;;
            *)
                echo "Unknown option: $1"
                exit 1
                ;;
        esac
    done
}

# ============================================================================
# Dynamic gsql wrapper generation (like Citus pg_regress_multi.pl)
# ============================================================================

# Detect ports from running Neon endpoints
detect_endpoint_ports() {
    local neon_local="${NEON_ROOT}/target/${BUILD_TYPE:-debug}/neon_local"
    
    if [[ -f "${neon_local}" ]] && [[ -d "${SCRIPT_DIR}/.neon" ]]; then
        log_info "Detecting endpoint ports from neon_local..."
        
        # Get endpoint list
        local endpoint_list
        endpoint_list=$(cd "${SCRIPT_DIR}" && "${neon_local}" endpoint list 2>/dev/null || true)
        
        if [[ -n "${endpoint_list}" ]]; then
            # Parse endpoint list to extract ports
            # Format: ENDPOINT  ADDRESS  TIMELINE  BRANCH_NAME  LSN  STATUS
            # Example: main  127.0.0.1:55432  abc123...  main  ---  running
            local idx=1
            while IFS= read -r line; do
                # Skip header line and empty lines
                [[ "$line" =~ ^[[:space:]]*ENDPOINT ]] && continue
                [[ -z "${line// }" ]] && continue
                
                local name addr port
                name=$(echo "${line}" | awk '{print $1}')
                addr=$(echo "${line}" | awk '{print $2}')
                
                # Skip if this looks like a header or invalid
                [[ "${name}" == "ENDPOINT" ]] && continue
                [[ "${addr}" == "ADDRESS" ]] && continue
                [[ -z "${addr}" ]] && continue
                
                # Extract port from address (e.g., 127.0.0.1:55432 -> 55432)
                port=$(echo "${addr}" | grep -oE '[0-9]+$')
                
                # Validate port is a number
                if ! [[ "${port}" =~ ^[0-9]+$ ]]; then
                    continue
                fi
                
                if [[ "${name}" == "main" ]]; then
                    MAIN_PORT="${port}"
                    PORT="${port}"
                    if $VERBOSE; then
                        log_info "  main -> port ${port}"
                    fi
                else
                    # Assign to branch ports in order
                    case ${idx} in
                        1) BRANCH1_PORT="${port}"
                           if $VERBOSE; then log_info "  branch1 (${name}) -> port ${port}"; fi
                           ;;
                        2) BRANCH2_PORT="${port}"
                           if $VERBOSE; then log_info "  branch2 (${name}) -> port ${port}"; fi
                           ;;
                        3) BRANCH3_PORT="${port}"
                           if $VERBOSE; then log_info "  branch3 (${name}) -> port ${port}"; fi
                           ;;
                    esac
                    ((idx++))
                fi
            done <<< "${endpoint_list}"
            
            if $VERBOSE; then
                log_info "Detected ports: main=${MAIN_PORT}, branch1=${BRANCH1_PORT}, branch2=${BRANCH2_PORT}, branch3=${BRANCH3_PORT}"
            fi
        fi
    fi
}

# Generate gsql wrapper script with preset variables (like Citus)
# This creates tmp-bin/gsql that wraps the real gsql with --variable arguments
generate_gsql_wrapper() {
    log_info "Generating gsql wrapper with preset variables..."
    
    # Create tmp-bin directory (like Citus's tmp_check/tmp-bin)
    mkdir -p "${TMP_BINDIR}"
    
    # Generate wrapper script
    local wrapper="${TMP_BINDIR}/gsql"
    
    cat > "${wrapper}" << EOF
#!/bin/bash
#
# Auto-generated gsql wrapper for Neon regression tests
# Generated at: $(date)
#
# This wrapper provides preset variables for multi-endpoint testing.
# Like Citus's dynamically generated gsql wrapper in pg_regress_multi.pl
#

# Real gsql path
GSQL_REAL="${GSQL_REAL}"

# Library path
export LD_LIBRARY_PATH="${OG_INSTALL}/${OG_VERSION}/lib:\${LD_LIBRARY_PATH:-}"

# Export environment variables for shell commands (\!)
export NEON_ROOT="${NEON_ROOT}"
export REGRESS_DIR="${SCRIPT_DIR}"
export MAIN_PORT="${MAIN_PORT}"
export BRANCH1_PORT="${BRANCH1_PORT}"
export BRANCH2_PORT="${BRANCH2_PORT}"
export BRANCH3_PORT="${BRANCH3_PORT}"
export PGSSLMODE="disable"

# Preset variables for SQL scripts (like Citus worker_1_port, worker_2_port)
exec "\${GSQL_REAL}" \\
    --variable=main_port=${MAIN_PORT} \\
    --variable=branch1_port=${BRANCH1_PORT} \\
    --variable=branch2_port=${BRANCH2_PORT} \\
    --variable=branch3_port=${BRANCH3_PORT} \\
    --variable=default_user=${USER} \\
    --variable=default_db=${DBNAME} \\
    --variable=default_host=${HOST} \\
    --variable=neon_root=${NEON_ROOT} \\
    --variable=regress_dir=${SCRIPT_DIR} \\
    "\$@"
EOF
    
    chmod +x "${wrapper}"
    
    if $VERBOSE; then
        log_info "Created wrapper: ${wrapper}"
        log_info "  main_port=${MAIN_PORT}"
        log_info "  branch1_port=${BRANCH1_PORT}"
        log_info "  branch2_port=${BRANCH2_PORT}"
        log_info "  branch3_port=${BRANCH3_PORT}"
    fi
    
    # Use the wrapper for tests
    GSQL="${wrapper}"
}

# ============================================================================
# Test Execution
# ============================================================================

# Run a single test
# Supports both SQL (.sql) and shell script (.sh) test cases
run_test() {
    local test_name="$1"
    local sql_file="${SQL_DIR}/${test_name}.sql"
    local sh_file="${SQL_DIR}/${test_name}.sh"
    local expected_file="${EXPECTED_DIR}/${test_name}.out"
    local result_file="${RESULTS_DIR}/${test_name}.out"
    local diff_file="${RESULTS_DIR}/${test_name}.diff"
    local test_type=""

    # Determine test type: shell script or SQL
    if [[ -f "${sh_file}" ]]; then
        test_type="shell"
        # Make sure script is executable
        chmod +x "${sh_file}" 2>/dev/null || true
    elif [[ -f "${sql_file}" ]]; then
        test_type="sql"
    else
        log_fail "${test_name}: Test file not found (checked ${sql_file} and ${sh_file})"
        return 1
    fi

    # Run the test
    if $VERBOSE; then
        log_info "Running ${test_type} test: ${test_name}"
    fi

    # Execute test based on type
    if [[ "${test_type}" == "shell" ]]; then
        # Execute shell script
        # For shell scripts (especially neon_env_setup), show output in real-time
        # Export environment variables for the script
        export HOST PORT USER DATABASE PG_VERSION BUILD_TYPE
        export OPENGAUSS_DISTRIB_DIR POSTGRES_DISTRIB_DIR DEFAULT_PG_VERSION
        export EXTRA_BRANCHES BRANCH_NAME ENDPOINT_PORT
        export MAIN_PORT BRANCH1_PORT BRANCH2_PORT BRANCH3_PORT
        export GSQL TMP_BINDIR
        
        # Use tee to show output in real-time AND save to file
        # This is especially important for neon_env_setup which takes time
        echo ""
        echo "=============================================="
        echo "  Running: ${test_name}"
        echo "=============================================="
        echo ""
        
        # IMPORTANT: Execute shell script from SCRIPT_DIR (regress_og directory)
        # This ensures .neon directory is created in the correct location
        if (cd "${SCRIPT_DIR}" && bash "${sh_file}" 2>&1) | tee "${result_file}"; then
            # Script succeeded - after env setup, regenerate wrapper with detected ports
            if [[ "${test_name}" == "neon_env_setup" ]]; then
                detect_endpoint_ports
                generate_gsql_wrapper
            fi
        else
            local exit_code=$?
            echo "Test script exited with code: ${exit_code}" | tee -a "${result_file}"
            return 1
        fi
    else
        # Execute SQL using the wrapper gsql (with preset variables)
        "${GSQL}" -h "${HOST}" -p "${PORT}" -U "${USER}" -d "${DBNAME}" \
            -a -f "${sql_file}" > "${result_file}" 2>&1 || true
    fi

    # Compare with expected output
    if [[ -f "${expected_file}" ]]; then
        # Use custom diff that applies normalization
        if "${BIN_DIR}/diff" -u "${expected_file}" "${result_file}" > "${diff_file}" 2>&1; then
            log_pass "${test_name}"
            rm -f "${diff_file}"
            return 0
        else
            log_fail "${test_name}"
            if $VERBOSE; then
                echo "  Diff saved to: ${diff_file}"
                head -20 "${diff_file}"
            fi
            return 1
        fi
    else
        log_warn "${test_name}: No expected file, creating one"
        # Create expected file from result for first run
        cp "${result_file}" "${expected_file}"
        return 0
    fi
}

# Parse schedule file and return list of tests
parse_schedule() {
    local schedule_file="$1"
    local tests=()

    while IFS= read -r line || [[ -n "$line" ]]; do
        # Skip comments and empty lines
        [[ "$line" =~ ^#.*$ ]] && continue
        [[ -z "${line// }" ]] && continue

        # Parse test: line
        if [[ "$line" =~ ^test:\ (.+)$ ]]; then
            local test_names="${BASH_REMATCH[1]}"
            for test_name in $test_names; do
                tests+=("$test_name")
            done
        fi
    done < "${schedule_file}"

    echo "${tests[@]}"
}

# Run all tests in a schedule
run_schedule() {
    local schedule_file="${SCRIPT_DIR}/${SCHEDULE}"

    if [[ ! -f "${schedule_file}" ]]; then
        log_fail "Schedule file not found: ${schedule_file}"
        exit 1
    fi

    log_info "Running schedule: ${SCHEDULE}"
    if [[ -n "${BRANCH_NAME}" ]]; then
        log_info "Branch: ${BRANCH_NAME}"
    fi
    log_info "Connection: ${USER}@${HOST}:${PORT}/${DBNAME}"

    local tests
    tests=$(parse_schedule "${schedule_file}")

    # Skip neon_env_setup if SKIP_ENV_SETUP is set (already run directly)
    if [[ "${SKIP_ENV_SETUP:-0}" == "1" ]]; then
        tests=$(echo "$tests" | sed 's/neon_env_setup//g')
        # Detect existing endpoints and generate wrapper
        detect_endpoint_ports
    fi
    
    # Generate the gsql wrapper with current port settings
    generate_gsql_wrapper

    # Add extra tests if specified
    if [[ -n "${EXTRA_TESTS}" ]]; then
        tests="${tests} ${EXTRA_TESTS}"
    fi

    local passed=0
    local failed=0
    local skipped=0

    for test_name in $tests; do
        if run_test "${test_name}"; then
            ((passed++))
        else
            ((failed++))
        fi
    done

    # Print summary
    echo ""
    echo "=============================================="
    echo "Test Summary"
    echo "=============================================="
    echo -e "  ${GREEN}Passed:${NC}  ${passed}"
    echo -e "  ${RED}Failed:${NC}  ${failed}"
    echo "  Total:   $((passed + failed))"
    echo "=============================================="
    echo ""
    log_info "gsql wrapper with preset variables: ${TMP_BINDIR}/gsql"
    log_info "Use this wrapper to run SQL files with port variables:"
    log_info "  ${TMP_BINDIR}/gsql -f sql/your_test.sql"

    if [[ ${failed} -gt 0 ]]; then
        echo ""
        log_info "Failed test diffs are in: ${RESULTS_DIR}/*.diff"
        return 1
    fi

    return 0
}

# ============================================================================
# Main
# ============================================================================

main() {
    parse_args "$@"

    # Verify real gsql exists
    if [[ ! -f "${GSQL_REAL}" ]]; then
        log_fail "gsql not found at ${GSQL_REAL}"
        exit 1
    fi

    # Setup results directory
    if ! $KEEP_RESULTS; then
        rm -rf "${RESULTS_DIR}"
    fi
    mkdir -p "${RESULTS_DIR}"

    # Make sure diff is executable
    chmod +x "${BIN_DIR}/diff" 2>/dev/null || true

    echo ""
    echo "=============================================="
    echo "  Neon Regression Tests for openGauss"
    echo "=============================================="
    echo ""

    if [[ -n "${SCHEDULE}" ]]; then
        run_schedule
    elif [[ -n "${TEST}" ]]; then
        # For single test, generate wrapper first
        detect_endpoint_ports
        generate_gsql_wrapper
        run_test "${TEST}"
    else
        log_fail "Please specify --schedule or --test"
        usage
    fi
}

main "$@"
