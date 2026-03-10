#!/usr/bin/env bash
#
# Neon Environment Cleanup Script
#
# This script stops all Neon services and cleans up the test environment:
#   - Stop all endpoints (compute nodes)
#   - Stop all Neon services (pageserver, safekeeper, broker, etc.)
#   - Remove the .neon directory
#   - Kill any orphaned processes
#
# Usage:
#   ./sql/neon_env_cleanup.sh           # Normal cleanup
#   ./sql/neon_env_cleanup.sh --force   # Force cleanup (kill processes)
#

set -euo pipefail

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REGRESS_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
NEON_ROOT="$(cd "${REGRESS_DIR}/../.." && pwd)"

# Configuration
BUILD_TYPE="${BUILD_TYPE:-debug}"
NEON_BIN="${NEON_ROOT}/target/${BUILD_TYPE}"
NEON_LOCAL="${NEON_BIN}/neon_local"

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1" >&2
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

# Parse arguments
FORCE_CLEANUP=false
for arg in "$@"; do
    case $arg in
        --force|-f)
            FORCE_CLEANUP=true
            shift
            ;;
    esac
done

echo ""
echo "=============================================="
echo "  Neon Environment Cleanup"
echo "=============================================="
echo ""

# Change to regress_og directory (where .neon should be)
cd "${REGRESS_DIR}"
log_info "Working directory: $(pwd)"

# ============================================================================
# Stop All Endpoints
# ============================================================================
log_info "Stopping all endpoints..."

if [[ -f "${NEON_LOCAL}" ]] && [[ -d ".neon" ]]; then
    # Get list of all endpoints
    endpoints=$("${NEON_LOCAL}" endpoint list 2>/dev/null | tail -n +2 | awk '{print $1}' || true)
    
    if [[ -n "${endpoints}" ]]; then
        for endpoint in ${endpoints}; do
            log_info "Stopping endpoint: ${endpoint}"
            "${NEON_LOCAL}" endpoint stop "${endpoint}" 2>/dev/null || log_warn "Could not stop ${endpoint}"
        done
    else
        log_info "No endpoints to stop"
    fi
else
    log_warn "neon_local not found or .neon directory doesn't exist"
fi

# ============================================================================
# Stop Neon Services
# ============================================================================
log_info "Stopping Neon services..."

if [[ -f "${NEON_LOCAL}" ]] && [[ -d ".neon" ]]; then
    "${NEON_LOCAL}" stop 2>/dev/null || log_warn "Could not stop Neon services (may not be running)"
else
    log_warn "neon_local not found or .neon directory doesn't exist"
fi

# ============================================================================
# Kill Orphaned Processes (if --force)
# ============================================================================
if ${FORCE_CLEANUP}; then
    log_info "Force cleanup: killing any orphaned Neon processes..."
    
    # Kill compute_ctl processes
    if pgrep -f "compute_ctl" > /dev/null 2>&1; then
        log_info "Killing compute_ctl processes..."
        pkill -f "compute_ctl" 2>/dev/null || true
    fi
    
    # Kill gaussdb processes (openGauss)
    if pgrep -f "gaussdb.*neon" > /dev/null 2>&1; then
        log_info "Killing gaussdb processes..."
        pkill -f "gaussdb.*neon" 2>/dev/null || true
    fi
    
    # Kill pageserver processes
    if pgrep -f "pageserver" > /dev/null 2>&1; then
        log_info "Killing pageserver processes..."
        pkill -f "pageserver" 2>/dev/null || true
    fi
    
    # Kill safekeeper processes
    if pgrep -f "safekeeper" > /dev/null 2>&1; then
        log_info "Killing safekeeper processes..."
        pkill -f "safekeeper" 2>/dev/null || true
    fi
    
    # Kill storage_broker processes
    if pgrep -f "storage_broker" > /dev/null 2>&1; then
        log_info "Killing storage_broker processes..."
        pkill -f "storage_broker" 2>/dev/null || true
    fi
    
    # Kill storage_controller processes
    if pgrep -f "storage_controller" > /dev/null 2>&1; then
        log_info "Killing storage_controller processes..."
        pkill -f "storage_controller" 2>/dev/null || true
    fi
    
    # Wait for processes to terminate
    sleep 2
fi

# ============================================================================
# Remove .neon Directory
# ============================================================================
log_info "Removing .neon directory..."

if [[ -d ".neon" ]]; then
    rm -rf ".neon"
    log_success ".neon directory removed"
else
    log_info ".neon directory does not exist"
fi

# ============================================================================
# Clean Results Directory (optional)
# ============================================================================
if [[ -d "results" ]]; then
    log_info "Cleaning results directory..."
    rm -rf results/*
    log_success "Results directory cleaned"
fi

# ============================================================================
# Verify Cleanup
# ============================================================================
log_info "Verifying cleanup..."

cleanup_success=true

# Check for remaining .neon directory
if [[ -d ".neon" ]]; then
    log_error ".neon directory still exists!"
    cleanup_success=false
fi

# Check for remaining processes
if pgrep -f "compute_ctl" > /dev/null 2>&1; then
    log_warn "Some compute_ctl processes are still running"
    if ${FORCE_CLEANUP}; then
        cleanup_success=false
    fi
fi

if pgrep -f "pageserver" > /dev/null 2>&1; then
    log_warn "Pageserver is still running"
    if ${FORCE_CLEANUP}; then
        cleanup_success=false
    fi
fi

if pgrep -f "safekeeper" > /dev/null 2>&1; then
    log_warn "Safekeeper is still running"
    if ${FORCE_CLEANUP}; then
        cleanup_success=false
    fi
fi

# ============================================================================
# Summary
# ============================================================================
echo ""
echo "=============================================="
if ${cleanup_success}; then
    log_success "Environment cleanup completed successfully!"
    echo "=============================================="
    echo ""
    echo "The following have been cleaned up:"
    echo "  - All Neon endpoints stopped"
    echo "  - All Neon services stopped"
    echo "  - .neon directory removed"
    echo "  - Results directory cleaned"
    echo ""
    echo "You can now run a fresh test with:"
    echo "  make check-basic"
    echo ""
else
    log_error "Environment cleanup completed with warnings!"
    echo "=============================================="
    echo ""
    echo "Some resources may not have been cleaned up."
    echo "Run with --force to kill remaining processes:"
    echo "  ./sql/neon_env_cleanup.sh --force"
    echo ""
    exit 1
fi

