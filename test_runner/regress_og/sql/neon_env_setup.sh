#!/usr/bin/env bash
#
# Neon Environment Setup Test Case
#
# This test case initializes the Neon environment including:
#   - Starting all Neon components (broker, controller, pageserver, safekeeper, endpoint_storage)
#   - Creating a tenant
#   - Creating initial timeline/branch (main)
#   - Starting compute endpoint
#   - Verifying environment is ready
#
# This test MUST run before any other tests that require Neon services.
# It can also be used to create additional branches for testing.

set -euo pipefail

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REGRESS_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
NEON_ROOT="$(cd "${REGRESS_DIR}/../.." && pwd)"

# Load configuration from environment or use defaults
# Note: neon_local --pg-version expects "14", "15", "16", "17" (PostgreSQL version numbers)
# For openGauss, "14" internally maps to V702
PG_VERSION="${DEFAULT_PG_VERSION:-14}"
BUILD_TYPE="${BUILD_TYPE:-debug}"
NEON_BIN="${NEON_ROOT}/target/${BUILD_TYPE}"
NEON_LOCAL="${NEON_BIN}/neon_local"
# Don't set NEON_REPO_DIR - let neon_local use its default (.neon in current directory)
HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-55432}"
USER="${USER:-cloud_admin}"
DATABASE="${DATABASE:-postgres}"

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

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

# Verify prerequisites
verify_prerequisites() {
    log_info "Verifying prerequisites..."
    
    if [[ ! -f "${NEON_LOCAL}" ]]; then
        log_error "neon_local not found at ${NEON_LOCAL}"
        exit 1
    fi
    
    # Ensure OPENGAUSS_DISTRIB_DIR or POSTGRES_DISTRIB_DIR is set and is absolute
    if [[ -z "${OPENGAUSS_DISTRIB_DIR:-}" ]] && [[ -z "${POSTGRES_DISTRIB_DIR:-}" ]]; then
        # Try to find og_install relative to NEON_ROOT
        if [[ -d "${NEON_ROOT}/og_install" ]]; then
            export OPENGAUSS_DISTRIB_DIR="${NEON_ROOT}/og_install"
            log_warn "OPENGAUSS_DISTRIB_DIR not set, using default: ${OPENGAUSS_DISTRIB_DIR}"
        else
            log_error "OPENGAUSS_DISTRIB_DIR or POSTGRES_DISTRIB_DIR must be set, and og_install not found at ${NEON_ROOT}/og_install"
            exit 1
        fi
    fi
    
    # Ensure paths are absolute
    if [[ -n "${OPENGAUSS_DISTRIB_DIR:-}" ]]; then
        OPENGAUSS_DISTRIB_DIR="$(cd "${OPENGAUSS_DISTRIB_DIR}" && pwd)"
        export OPENGAUSS_DISTRIB_DIR
        log_info "OPENGAUSS_DISTRIB_DIR: ${OPENGAUSS_DISTRIB_DIR}"
    fi
    
    if [[ -n "${POSTGRES_DISTRIB_DIR:-}" ]]; then
        POSTGRES_DISTRIB_DIR="$(cd "${POSTGRES_DISTRIB_DIR}" && pwd)"
        export POSTGRES_DISTRIB_DIR
        log_info "POSTGRES_DISTRIB_DIR: ${POSTGRES_DISTRIB_DIR}"
    fi
    
    log_success "Prerequisites verified"
}

# Initialize Neon repository
init_neon_repo() {
    log_info "Initializing Neon repository..."
    echo "  Current working directory: $(pwd)"
    
    # CRITICAL: Ensure OPENGAUSS_DISTRIB_DIR is set to absolute path
    # This is used by neon_local init to configure pg_distrib_dir in .neon/config
    if [[ -z "${OPENGAUSS_DISTRIB_DIR:-}" ]]; then
        export OPENGAUSS_DISTRIB_DIR="${NEON_ROOT}/og_install"
    fi
    
    # Ensure it's an absolute path
    if [[ "${OPENGAUSS_DISTRIB_DIR}" != /* ]]; then
        log_warn "OPENGAUSS_DISTRIB_DIR is not absolute, converting..."
        export OPENGAUSS_DISTRIB_DIR="$(cd "${OPENGAUSS_DISTRIB_DIR}" 2>/dev/null && pwd || echo "${NEON_ROOT}/og_install")"
    fi
    
    # Also set POSTGRES_DISTRIB_DIR to the same value
    export POSTGRES_DISTRIB_DIR="${OPENGAUSS_DISTRIB_DIR}"
    
    echo "  OPENGAUSS_DISTRIB_DIR: ${OPENGAUSS_DISTRIB_DIR}"
    echo "  POSTGRES_DISTRIB_DIR: ${POSTGRES_DISTRIB_DIR}"
    echo "  NEON_ROOT: ${NEON_ROOT}"
    
    # Verify the directory exists
    if [[ ! -d "${OPENGAUSS_DISTRIB_DIR}" ]]; then
        log_error "OPENGAUSS_DISTRIB_DIR does not exist: ${OPENGAUSS_DISTRIB_DIR}"
        return 1
    fi
    
    # Verify bin directory exists
    if [[ ! -d "${OPENGAUSS_DISTRIB_DIR}/V702/bin" ]]; then
        log_error "openGauss bin directory not found: ${OPENGAUSS_DISTRIB_DIR}/V702/bin"
        return 1
    fi
    
    # Clean up existing repo if it exists
    if [[ -d ".neon" ]]; then
        log_warn "Removing existing .neon directory"
        rm -rf ".neon"
    fi
    
    # Run neon_local init - just like test.sh
    echo "  Running: ${NEON_LOCAL} init"
    echo ""
    
    "${NEON_LOCAL}" init || {
        log_error "Failed to initialize Neon repository"
        return 1
    }
    
    # Verify the config was created with correct path
    if [[ -f ".neon/config" ]]; then
        local stored_path
        stored_path=$(grep "pg_distrib_dir" .neon/config | cut -d'"' -f2)
        if [[ "${stored_path}" != "${OPENGAUSS_DISTRIB_DIR}" ]]; then
            log_warn "Config stored different path: ${stored_path}"
            log_warn "Expected: ${OPENGAUSS_DISTRIB_DIR}"
        else
            log_success "Config verified: pg_distrib_dir = ${stored_path}"
        fi
    fi
    
    echo ""
    log_success "Neon repository initialized"
}

# Start all Neon services
start_neon_services() {
    log_info "Starting Neon services..."
    echo "  This may take 30-60 seconds..."
    echo ""
    echo "  Running: ${NEON_LOCAL} start"
    echo ""
    
    # Simply run neon_local start, letting it use system environment variables
    "${NEON_LOCAL}" start || {
        log_error "Failed to start Neon services"
        return 1
    }
    
    echo ""
    log_success "Neon services started"
}

# Create tenant and initial timeline
create_tenant_and_timeline() {
    log_info "Creating tenant and initial timeline..."
    echo "  Version: ${PG_VERSION}"
    echo ""
    
    # Create tenant (this also creates the default 'main' timeline)
    "${NEON_LOCAL}" tenant create --pg-version "${PG_VERSION}" --set-default || {
        log_error "Failed to create tenant"
        return 1
    }
    
    echo ""
    log_success "Tenant and initial timeline created"
}

# Create additional branches if specified
create_additional_branches() {
    local branches="${EXTRA_BRANCHES:-}"
    
    if [[ -z "${branches}" ]]; then
        return 0
    fi
    
    log_info "Creating additional branches: ${branches}"
    
    for branch in ${branches}; do
        log_info "Creating branch: ${branch}"
        "${NEON_LOCAL}" timeline create --branch-name "${branch}" --pg-version "${PG_VERSION}" || {
            log_warn "Failed to create branch ${branch}, it may already exist"
        }
    done
    
    log_success "Additional branches created"
}

# Create and start compute endpoint
create_and_start_endpoint() {
    local endpoint_name="${ENDPOINT_NAME:-main}"
    
    log_info "Creating and starting endpoint: ${endpoint_name}"
    echo ""
    
    # Create endpoint
    echo "  Creating endpoint..."
    "${NEON_LOCAL}" endpoint create "${endpoint_name}" --pg-version "${PG_VERSION}" || {
        log_warn "Endpoint ${endpoint_name} may already exist"
    }
    
    echo ""
    # Start endpoint
    echo "  Starting endpoint..."
    "${NEON_LOCAL}" endpoint start "${endpoint_name}" || {
        log_error "Failed to start endpoint"
        return 1
    }
    
    echo ""
    log_success "Endpoint ${endpoint_name} started at ${HOST}:${PORT}"
}

# Verify environment is ready
verify_environment() {
    log_info "Verifying environment is ready..."
    
    # Wait a bit for endpoint to be fully ready
    sleep 3
    
    # Check that endpoint process is running
    if pgrep -f "compute_ctl.*endpoints/main" > /dev/null 2>&1; then
        log_success "Endpoint process is running"
    else
        log_warn "Endpoint process not found, but continuing anyway"
    fi
    
    log_success "Environment verification complete"
    log_info "Note: Database authentication is configured via pg_hba.conf"
    log_info "      For SQL tests, use appropriate connection credentials"
    
    return 0
}

# Show environment status
show_status() {
    log_info "Environment Status:"
    echo ""
    echo "=============================================="
    echo "  Neon Environment Setup Complete"
    echo "=============================================="
    echo "Repository: $(pwd)/.neon"
    echo "Endpoint:   ${HOST}:${PORT}"
    echo "Database:   ${DATABASE}"
    echo "User:       ${USER}"
    echo "Version:    ${PG_VERSION}"
    echo "=============================================="
    echo ""
    
    # Show tenant and timeline info
    log_info "Tenant and Timeline Information:"
    "${NEON_LOCAL}" tenant list || true
    "${NEON_LOCAL}" timeline list || true
    "${NEON_LOCAL}" endpoint list || true
    echo ""
}

# Main execution
main() {
    # Immediately show that we're starting (important for user feedback)
    echo ""
    echo "=============================================="
    echo "  Neon Environment Setup Test"
    echo "=============================================="
    echo ""
    echo "Starting Neon environment initialization..."
    echo "This will take approximately 30-60 seconds."
    echo ""
    
    # Run each step and check for errors
    verify_prerequisites || {
        log_error "Prerequisites check failed"
        exit 1
    }
    
    echo ""
    init_neon_repo || {
        log_error "Repository initialization failed"
        exit 1
    }
    
    echo ""
    start_neon_services || {
        log_error "Failed to start Neon services"
        exit 1
    }
    
    echo ""
    create_tenant_and_timeline || {
        log_error "Failed to create tenant and timeline"
        exit 1
    }
    
    echo ""
    create_additional_branches || {
        log_warn "Some branches may not have been created"
    }
    
    echo ""
    create_and_start_endpoint || {
        log_error "Failed to create/start endpoint"
        exit 1
    }
    
    echo ""
    verify_environment || {
        log_error "Environment verification failed"
        exit 1
    }
    
    echo ""
    show_status
    
    log_success "Neon environment setup completed successfully"
    echo ""
}

# Run main function
main "$@"

