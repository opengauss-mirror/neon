# pgxs/neon/Makefile


MODULE_big = neon_rmgr
OBJS = \
	$(WIN32RES) \
	neon_rmgr.o \
	neon_rmgr_decode.o \
	neon_rmgr_desc.o

EXTENSION = neon_rmgr
DATA = neon_rmgr.control

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# Override srcdir and VPATH for VPATH builds
srcdir = $(abspath $(dir $(firstword $(MAKEFILE_LIST))))
VPATH = $(srcdir)

# OpenGauss headers require C++ compilation for default parameters and cxxabi.h
override CXXFLAGS += -fPIC
%.o: %.c
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) -c -o $@ $<
