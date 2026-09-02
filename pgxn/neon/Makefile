# pgxs/neon/Makefile


MODULE_big = neon
OBJS = \
	$(WIN32RES) \
	communicator.o \
	communicator_process.o \
	extension_server.o \
	file_cache.o \
	hll.o \
	libpagestore.o \
	logical_replication_monitor.o \
	neon.o \
	neon_lwlsncache.o \
	neon_pgversioncompat.o \
	neon_perf_counters.o \
	neon_utils.o \
	neon_walreader.o \
	oggit_merge_barrier.o \
	oggit_worker.o \
	pagestore_smgr.o \
	relsize_cache.o \
	unstable_extensions.o \
	walproposer.o \
	walproposer_pg.o \
	neon_ddl_handler.o \
	walsender_hooks.o \
	$(NEON_CARGO_ARTIFACT_TARGET_DIR)/libcommunicator.a \
	spq_io_multiplex.o \
	wait_events.o

PG_CPPFLAGS += -I$(libpq_srcdir)
SHLIB_LINK_INTERNAL = $(libpq)
SHLIB_LINK = -lcurl

UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S), Darwin)
    SHLIB_LINK += -framework Security -framework CoreFoundation -framework SystemConfiguration

    # Link against object files for the current macOS version, to avoid spurious linker warnings.
    MACOSX_DEPLOYMENT_TARGET := $(shell xcrun --sdk macosx --show-sdk-version)
    export MACOSX_DEPLOYMENT_TARGET
endif

EXTENSION = neon
DATA = \
	neon--1.0.sql \
	neon.control
PGFILEDESC = "neon - cloud storage for PostgreSQL"

EXTRA_CLEAN = \
	libwalproposer.a

AROPT ?= rcs

WALPROP_OBJS = \
	$(WIN32RES) \
	walproposer.o \
	neon_utils.o \
	walproposer_compat.o

# libcommunicator.a is built by cargo from the Rust sources under communicator/
# subdirectory. `cargo build` also generates communicator_bindings.h.
communicator_process.o: communicator/communicator_bindings.h
file_cache.o: communicator/communicator_bindings.h

$(NEON_CARGO_ARTIFACT_TARGET_DIR)/libcommunicator.a communicator/communicator_bindings.h &:
	(cd $(srcdir)/communicator && cargo build $(CARGO_BUILD_FLAGS) $(CARGO_PROFILE))

# Force `cargo build` every time. Some of the Rust sources might have
# changed.
.PHONY: $(NEON_CARGO_ARTIFACT_TARGET_DIR)/libcommunicator.a communicator/communicator_bindings.h

.PHONY: walproposer-lib
walproposer-lib: CPPFLAGS += -DWALPROPOSER_LIB
walproposer-lib: libwalproposer.a;

.PHONY: libwalproposer.a
libwalproposer.a: $(WALPROP_OBJS)
	$(RM) $@
	$(AR) $(AROPT) $@ $^

# needs vars:
# FIND_TYPEDEF pointing to find_typedef
# INDENT pointing to pg_bsd_indent
# PGINDENT_SCRIPT pointing to pgindent (be careful with PGINDENT var name:
#   pgindent will pick it up as pg_bsd_indent path).
.PHONY: pgindent
pgindent:
	+@ echo top_srcdir=$(top_srcdir) top_builddir=$(top_builddir) srcdir=$(srcdir)
	$(FIND_TYPEDEF) . > neon.typedefs
	INDENT=$(INDENT) $(PGINDENT_SCRIPT) --typedefs neon.typedefs $(srcdir)/*.c $(srcdir)/*.h
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
srcdir = $(abspath $(dir $(firstword $(MAKEFILE_LIST))))
VPATH = $(srcdir)

NEON_KERNEL_CFLAGS := $(shell $(PG_CONFIG) --cflags)

NEON_MACROS := $(filter -D%,$(NEON_KERNEL_CFLAGS))

# Remove -fPIE from NEON_KERNEL_CFLAGS as we need -fPIC for shared libraries
# -fPIE is for executables, -fPIC is for shared libraries
NEON_KERNEL_CFLAGS := $(filter-out -fPIE,$(NEON_KERNEL_CFLAGS))

# NEON_KERNEL_CFLAGS already contains -DENABLE_NEON from pg_config --cflags,
# but we use override to ensure it's not overridden by PGXS or command line
override CXXFLAGS += $(NEON_KERNEL_CFLAGS) -fPIC -DPGXC -I/$(CODE_BASE)/src/include

override CXXFLAGS += -g3 -fno-omit-frame-pointer

# Add Python include path for plpython.h
# Python.h is located in OPENGAUSS_BINARYLIBS_DIR/kernel/platform/python3.7/include/python3.7m/
# Default to the neon project's own binarylibs if OPENGAUSS_BINARYLIBS_DIR is not set.
OPENGAUSS_BINARYLIBS_DIR ?= /usr1/neon/3rd/openGauss-third_party_binarylibs_openEuler_2203_arm
PYTHON_INCLUDE_PATH := -I$(OPENGAUSS_BINARYLIBS_DIR)/kernel/platform/python3.7/include/python3.7m
override CXXFLAGS += $(PYTHON_INCLUDE_PATH)

%.o: %.c
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) -c -o $@ $<
