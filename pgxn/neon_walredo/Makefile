# pgxs/neon_walredo/Makefile

MODULE_big = neon_walredo
OBJS = \
	$(WIN32RES) \
	inmem_smgr.o \
	walredoproc.o \

# This really should be guarded by $(with_libseccomp), but I couldn't
# make that work with pgxs. So we always compile it, but its contents
# are wrapped in #ifdef HAVE_LIBSECCOMP instead.
OBJS += seccomp.o

PGFILEDESC = "neon_walredo - helper process that runs in Neon pageserver"

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

override srcdir := $(dir $(firstword $(MAKEFILE_LIST)))
override VPATH := $(srcdir)

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
PYTHON_INCLUDE_PATH := -I$(OPENGAUSS_BINARYLIBS_DIR)/kernel/platform/python3.7/include/python3.7m
override CXXFLAGS += $(PYTHON_INCLUDE_PATH)

%.o: %.c
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) -c -o $@ $<

ifeq ($(with_libseccomp),yes)
SHLIB_LINK += -lseccomp
endif

