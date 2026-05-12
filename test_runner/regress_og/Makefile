# Makefile for Neon Regression Tests (openGauss)
#
# This Makefile provides targets for running different test suites.
# It follows the pattern used by Citus/PostgreSQL regression tests.
#
# Usage:
#   make check-basic                # Basic functionality tests
#   make check-branch-inherit       # Branch data inheritance tests
#   make clean-all                  # Clean everything

# ============================================================================
# Configuration
# ============================================================================

# Base directories - use absolute paths
REGRESS_DIR := $(shell pwd)
NEON_ROOT := $(shell cd $(REGRESS_DIR)/../.. && pwd)
OG_INSTALL := $(NEON_ROOT)/og_install
# OG_VERSION: openGauss directory version (e.g., V702)
OG_VERSION ?= V702
# PG_VERSION: PostgreSQL major version number for neon_local (14 maps to V702 internally)
PG_VERSION ?= 14
# Auto-detect BUILD_TYPE: prefer release if available, otherwise debug
_BUILD_TYPE := $(shell test -f $(NEON_ROOT)/target/release/neon_local && echo release || echo debug)
BUILD_TYPE ?= $(_BUILD_TYPE)

# Binaries
NEON_BIN := $(NEON_ROOT)/target/$(BUILD_TYPE)
GSQL := $(OG_INSTALL)/$(OG_VERSION)/bin/gsql
NEON_LOCAL := $(NEON_BIN)/neon_local

# Test directories
SQL_DIR := $(REGRESS_DIR)/sql
EXPECTED_DIR := $(REGRESS_DIR)/expected
RESULTS_DIR := $(REGRESS_DIR)/results
BIN_DIR := $(REGRESS_DIR)/bin

# Add custom bin directory to PATH for custom diff
export PATH := $(BIN_DIR):$(PATH)

# Connection settings (can be overridden)
HOST ?= 127.0.0.1
PORT ?= 55432
USER ?= cloud_admin
DATABASE ?= postgres

# Environment - must use absolute paths for neon_local
export LD_LIBRARY_PATH := $(OG_INSTALL)/$(OG_VERSION)/lib:$(LD_LIBRARY_PATH)
export OPENGAUSS_DISTRIB_DIR := $(OG_INSTALL)
export POSTGRES_DISTRIB_DIR := $(OG_INSTALL)
export DEFAULT_PG_VERSION := $(PG_VERSION)
export DEFAULT_OG_VERSION := $(OG_VERSION)

# ============================================================================
# Main Test Targets
# ============================================================================

.PHONY: all check check-basic check-branch-inherit check-large-data setup verify-env

# Default target
all: check-basic

# Run all verified tests
check: check-basic

# Basic functionality tests
check-basic: setup
	@echo "=== Running Basic Functionality Tests ==="
	@echo "Starting Neon environment (this will take 30-60 seconds)..."
	@bash sql/neon_env_setup.sh || (echo "ERROR: Neon environment setup failed"; exit 1)
	@echo ""
	@echo "Running basic tests..."
	@./run_tests.sh --schedule=basic_schedule --host=$(HOST) --port=$(PORT) --skip-env-setup --verbose

# Branch data inheritance test (creates branch and tests data isolation)
check-branch-inherit: setup
	@echo "=== Running Branch Data Inheritance Test ==="
	@echo "Starting Neon environment (this will take 30-60 seconds)..."
	@bash sql/neon_env_setup.sh || (echo "ERROR: Neon environment setup failed"; exit 1)
	@echo ""
	@echo "Running branch inheritance test..."
	@./run_tests.sh --schedule=branch_inherit_schedule --host=$(HOST) --port=$(PORT) --skip-env-setup --verbose

# Large data tests (large data volume operations and performance)
check-large-data: setup
	@echo "=== Running Large Data Tests ==="
	@echo "Starting Neon environment (this will take 30-60 seconds)..."
	@bash sql/neon_env_setup.sh || (echo "ERROR: Neon environment setup failed"; exit 1)
	@echo ""
	@echo "Running large data tests..."
	@./run_tests.sh --schedule=schedules/large_data_schedule --host=$(HOST) --port=$(PORT) --skip-env-setup --verbose


# ============================================================================
# Environment Management
# ============================================================================

.PHONY: start-neon stop-neon stop-neon-force status-neon

# Verify environment is ready
verify-env:
	@echo "Verifying environment..."
	@test -f $(GSQL) || (echo "ERROR: gsql not found at $(GSQL)" && exit 1)
	@test -f $(NEON_LOCAL) || (echo "ERROR: neon_local not found at $(NEON_LOCAL)" && exit 1)
	@echo "Environment verified."

# Setup test environment
setup: verify-env
	@mkdir -p $(RESULTS_DIR)
	@mkdir -p $(EXPECTED_DIR)
	@chmod +x $(BIN_DIR)/diff 2>/dev/null || true

# Start Neon (using the setup script)
start-neon: setup
	@echo "Starting Neon environment..."
	@bash sql/neon_env_setup.sh

# Stop Neon services
stop-neon:
	@echo "Stopping Neon services..."
	-$(NEON_LOCAL) stop 2>/dev/null || true
	@echo "Neon stopped."

# Stop all services and clean up .neon directory
stop-neon-force:
	@echo "Force stopping all Neon services..."
	@bash sql/neon_env_cleanup.sh --force

# Show Neon status
status-neon:
	@echo "=== Neon Status ==="
	-$(NEON_LOCAL) endpoint list 2>/dev/null || echo "Neon not running"
	@echo ""

# ============================================================================
# Cleanup
# ============================================================================

.PHONY: clean clean-results clean-all

# Clean test results only
clean-results:
	rm -rf $(RESULTS_DIR)/*
	rm -f regression.diffs regression.out

# Clean test results and stop Neon
clean: clean-results
	-$(MAKE) stop-neon 2>/dev/null || true

# Clean everything including Neon repo (full cleanup)
clean-all:
	@echo "Performing full cleanup..."
	@bash sql/neon_env_cleanup.sh --force 2>/dev/null || true
	rm -rf .neon
	rm -rf $(RESULTS_DIR)
	rm -rf tmp-bin

# ============================================================================
# Development Helpers
# ============================================================================

.PHONY: update-expected list-tests list-schedules run-test

# Update expected output from results
update-expected:
	@if [ -z "$(TEST)" ]; then \
		echo "Usage: make update-expected TEST=<test_name>"; \
		exit 1; \
	fi
	@if [ -f $(RESULTS_DIR)/$(TEST).out ]; then \
		cp $(RESULTS_DIR)/$(TEST).out $(EXPECTED_DIR)/$(TEST).out; \
		echo "Updated expected/$(TEST).out"; \
	else \
		echo "ERROR: $(RESULTS_DIR)/$(TEST).out not found"; \
		echo "Run the test first: make run-test TEST=$(TEST)"; \
		exit 1; \
	fi

# Update all expected outputs from results
update-all-expected:
	@echo "Updating all expected outputs from results..."
	@for f in $(RESULTS_DIR)/*.out; do \
		if [ -f "$$f" ]; then \
			name=$$(basename $$f); \
			cp "$$f" "$(EXPECTED_DIR)/$$name"; \
			echo "Updated: $$name"; \
		fi \
	done

# List available tests
list-tests:
	@echo "Available tests:"
	@echo "================"
	@ls -1 $(SQL_DIR)/*.sql 2>/dev/null | xargs -n1 basename | sed 's/\.sql$$//' | sort

# List available schedules
list-schedules:
	@echo "Available schedules:"
	@echo "==================="
	@ls -1 $(REGRESS_DIR)/*_schedule 2>/dev/null | xargs -n1 basename | sort

# Run a specific test (manual mode, Neon must be running)
run-test: setup
	@if [ -z "$(TEST)" ]; then \
		echo "Usage: make run-test TEST=<test_name>"; \
		exit 1; \
	fi
	@echo "Running test: $(TEST)"
	@./run_tests.sh --test=$(TEST) --host=$(HOST) --port=$(PORT) --verbose

# ============================================================================
# Help
# ============================================================================

.PHONY: help

help:
	@echo "=============================================="
	@echo "  Neon Regression Tests for openGauss"
	@echo "=============================================="
	@echo ""
	@echo "Usage: make [target] [options]"
	@echo ""
	@echo "Test Targets:"
	@echo "  check-basic              Basic functionality tests"
	@echo "  check-branch-inherit     Branch data inheritance test"
	@echo "  check                    Alias for check-basic"
	@echo ""
	@echo "Environment Management:"
	@echo "  start-neon               Start Neon environment"
	@echo "  stop-neon                Stop Neon services"
	@echo "  stop-neon-force          Force stop (kill orphaned processes)"
	@echo "  status-neon              Show Neon status"
	@echo ""
	@echo "Development:"
	@echo "  list-tests               List available tests"
	@echo "  list-schedules           List available schedules"
	@echo "  run-test TEST=x          Run single test (Neon must be running)"
	@echo "  update-expected TEST=x   Update expected from results"
	@echo "  update-all-expected      Update all expected outputs"
	@echo ""
	@echo "Cleanup:"
	@echo "  clean                    Clean results and stop Neon"
	@echo "  clean-results            Clean only test results"
	@echo "  clean-all                Clean everything"
	@echo ""
	@echo "Options:"
	@echo "  HOST=<host>              Database host (default: 127.0.0.1)"
	@echo "  PORT=<port>              Database port (default: 55432)"
	@echo "  OG_VERSION=<ver>         openGauss directory version (default: V702)"
	@echo "  PG_VERSION=<ver>         PostgreSQL version for neon_local (default: 14)"
	@echo "  BUILD_TYPE=<type>        Build type (auto-detected: release or debug)"
	@echo ""
	@echo "Examples:"
	@echo "  make check-basic         # Run basic tests"
	@echo "  make check-branch-inherit # Run branch inheritance test"
	@echo "  make run-test TEST=neon_basic"
	@echo "  make update-expected TEST=neon_basic"
