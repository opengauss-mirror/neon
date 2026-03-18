ROOT_PROJECT_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Where to install openGauss, default is ./og_install
OPENGAUSS_INSTALL_DIR ?= $(ROOT_PROJECT_DIR)og_install
# Normalize to absolute path
OPENGAUSS_INSTALL_DIR := $(abspath $(OPENGAUSS_INSTALL_DIR))

# Path to openGauss binarylibs directory, default is 3rd_og
OPENGAUSS_BINARYLIBS_DIR ?= $(THIRD_BIN_PATH)

# Supported openGauss versions
OPENGAUSS_VERSIONS = V702

# CARGO_BUILD_FLAGS: Extra flags to pass to `cargo build`. `--locked`
# and `--features testing` are popular examples.
#
# CARGO_PROFILE: Set to override the cargo profile to use. By default,
# it is derived from BUILD_TYPE.

# All intermediate build artifacts are stored here.
BUILD_DIR := $(ROOT_PROJECT_DIR)/build

ICU_PREFIX_DIR := /usr/local/icu

#
# We differentiate between release / debug build types using the BUILD_TYPE
# environment variable.
#
BUILD_TYPE ?= release
WITH_SANITIZERS ?= no
PG_CFLAGS = -fsigned-char
ifeq ($(BUILD_TYPE),release)
	PG_CONFIGURE_OPTS = --enable-debug --with-openssl
	PG_CFLAGS += -O2 -g3 $(CFLAGS)
	PG_LDFLAGS = $(LDFLAGS)
	CARGO_PROFILE ?= --profile=release
	# NEON_CARGO_ARTIFACT_TARGET_DIR is the directory where `cargo build` places
	# the final build artifacts. There is unfortunately no easy way of changing
	# it to a fully predictable path, nor to extract the path with a simple
	# command. See https://github.com/rust-lang/cargo/issues/9661 and
	# https://github.com/rust-lang/cargo/issues/6790.
	NEON_CARGO_ARTIFACT_TARGET_DIR = $(ROOT_PROJECT_DIR)/target/release
else ifeq ($(BUILD_TYPE),debug)
	PG_CONFIGURE_OPTS = --enable-debug --with-openssl --enable-cassert --enable-depend
	PG_CFLAGS += -O0 -g3 $(CFLAGS)
	PG_LDFLAGS = $(LDFLAGS)
	CARGO_PROFILE ?= --profile=dev
	NEON_CARGO_ARTIFACT_TARGET_DIR = $(ROOT_PROJECT_DIR)/target/debug
else
	$(error Bad build type '$(BUILD_TYPE)', see Makefile for options)
endif

ifeq ($(WITH_SANITIZERS),yes)
	PG_CFLAGS += -fsanitize=address -fsanitize=undefined -fno-sanitize-recover
	COPT += -Wno-error # to avoid failing on warnings induced by sanitizers
	PG_LDFLAGS = -fsanitize=address -fsanitize=undefined -static-libasan -static-libubsan $(LDFLAGS)
	export CC := gcc
	export ASAN_OPTIONS := detect_leaks=0
endif

ifeq ($(shell test -e /home/nonroot/.docker_build && echo -n yes),yes)
	# Exclude static build openssl, icu for local build (MacOS, Linux)
	# Only keep for build type release and debug
	PG_CONFIGURE_OPTS += --with-icu
	PG_CONFIGURE_OPTS += ICU_CFLAGS='-I/$(ICU_PREFIX_DIR)/include -DU_STATIC_IMPLEMENTATION'
	PG_CONFIGURE_OPTS += ICU_LIBS='-L$(ICU_PREFIX_DIR)/lib -L$(ICU_PREFIX_DIR)/lib64 -licui18n -licuuc -licudata -lstdc++ -Wl,-Bdynamic -lm'
endif

UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Linux)
	# Seccomp BPF is only available for Linux
	ifneq ($(WITH_SANITIZERS),yes)
		PG_CONFIGURE_OPTS += --with-libseccomp
	endif
else ifeq ($(UNAME_S),Darwin)
	PG_CFLAGS += -DUSE_PREFETCH
	ifndef DISABLE_HOMEBREW
		# macOS with brew-installed openssl requires explicit paths
		# It can be configured with OPENSSL_PREFIX variable
		OPENSSL_PREFIX := $(shell brew --prefix openssl@3)
		PG_CONFIGURE_OPTS += --with-includes=$(OPENSSL_PREFIX)/include --with-libraries=$(OPENSSL_PREFIX)/lib
		PG_CONFIGURE_OPTS += PKG_CONFIG_PATH=$(shell brew --prefix icu4c)/lib/pkgconfig
		# macOS already has bison and flex in the system, but they are old
		# brew formulae are keg-only and not symlinked into HOMEBREW_PREFIX, force their usage
		EXTRA_PATH_OVERRIDES += $(shell brew --prefix bison)/bin/:$(shell brew --prefix flex)/bin/:
	endif
endif

# Use -C option so that when openGauss "make install" installs the
# headers, the mtime of the headers are not changed when there have
# been no changes to the files. Changing the mtime triggers an
# unnecessary rebuild of 'postgres_ffi'.
PG_CONFIGURE_OPTS += INSTALL='$(ROOT_PROJECT_DIR)/scripts/ninstall.sh -C'

MAKEFLAGS += -j
# Choose whether we should be silent or verbose
CARGO_BUILD_FLAGS += --$(if $(filter s,$(MAKEFLAGS)),quiet,verbose)
# Fix for a corner case when make doesn't pass a jobserver
CARGO_BUILD_FLAGS += $(filter -j`nproc`,$(MAKEFLAGS))

# This option has a side effect of passing make jobserver to cargo.
# However, we shouldn't do this if `make -n` (--dry-run) has been asked.
CARGO_CMD_PREFIX += $(if $(filter n,$(MAKEFLAGS)),,+)
# Force cargo not to print progress bar
CARGO_CMD_PREFIX += CARGO_TERM_PROGRESS_WHEN=never CI=1

CACHEDIR_TAG_CONTENTS := "Signature: 8a477f597d28d172789f06886806bc55"

#
# Top level Makefile to build Neon and openGauss
#
.PHONY: all
all: neon neon-pg-ext

.PHONY: show-config
show-config:
	@echo "BUILD_TYPE=$(BUILD_TYPE) TARGET=$(NEON_CARGO_ARTIFACT_TARGET_DIR)"

### Neon Rust bits
#
# The 'postgres_ffi' crate depends on the openGauss headers.
.PHONY: neon
neon: walproposer-lib cargo-target-dir
	$(CARGO_CMD_PREFIX) cargo build $(CARGO_BUILD_FLAGS) $(CARGO_PROFILE)

.PHONY: cargo-target-dir
cargo-target-dir:
	# https://github.com/rust-lang/cargo/issues/14281
	mkdir -p target
	test -e target/CACHEDIR.TAG || echo "$(CACHEDIR_TAG_CONTENTS)" > target/CACHEDIR.TAG

.PHONY: neon-pg-ext-%
neon-pg-ext-%: opengauss-build-% cargo-target-dir
	@mkdir -p $(BUILD_DIR)/pgxn-$* \
		$(OPENGAUSS_INSTALL_DIR)/$*/lib/postgresql \
		$(OPENGAUSS_INSTALL_DIR)/$*/share/postgresql/extension
	@PATH="$(OPENGAUSS_INSTALL_DIR)/$*/bin:$$PATH" \
	$(MAKE) -s PG_CONFIG="$(OPENGAUSS_INSTALL_DIR)/$*/bin/pg_config" COPT='$(COPT)' \
		NEON_CARGO_ARTIFACT_TARGET_DIR="$(NEON_CARGO_ARTIFACT_TARGET_DIR)" \
		CARGO_BUILD_FLAGS="$(CARGO_BUILD_FLAGS)" \
		CARGO_PROFILE="$(CARGO_PROFILE)" \
		-C $(BUILD_DIR)/pgxn-$* -f $(ROOT_PROJECT_DIR)pgxn/Makefile install
	@for f in $(BUILD_DIR)/pgxn-$*/neon*/*.so; do \
		[ -f "$$f" ] && cp -f "$$f" $(OPENGAUSS_INSTALL_DIR)/$*/lib/postgresql/; \
	done 2>/dev/null || true
	@for f in $(ROOT_PROJECT_DIR)pgxn/neon*/*.{control,sql}; do \
		[ -f "$$f" ] && cp -f "$$f" $(OPENGAUSS_INSTALL_DIR)/$*/share/postgresql/extension/; \
	done 2>/dev/null || true
	@[ -f "$(OPENGAUSS_INSTALL_DIR)/$*/lib/postgresql/neon.so" ] || \
		(echo "ERROR: neon.so not found" && exit 1)

# Build walproposer as a static library. walproposer source code is located
# in the pgxn/neon directory.
#
# We also need to include libpgport.a and libpgcommon.a, because walproposer
# uses some functions from those libraries.
#
# Some object files are removed from libpgport.a and libpgcommon.a because
# they depend on openssl and other libraries that are not included in our
# Rust build.
.PHONY: walproposer-lib
walproposer-lib: neon-pg-ext-V702
	+@echo "Compiling walproposer-lib"
	mkdir -p $(BUILD_DIR)/walproposer-lib
	$(MAKE) PG_CONFIG=$(OPENGAUSS_INSTALL_DIR)/V702/bin/pg_config COPT='$(COPT)' \
		-C $(BUILD_DIR)/walproposer-lib \
		-f $(ROOT_PROJECT_DIR)/pgxn/neon/Makefile walproposer-lib
	cp $(OPENGAUSS_INSTALL_DIR)/V702/lib/libpgport.a $(BUILD_DIR)/walproposer-lib
	# cp $(OPENGAUSS_INSTALL_DIR)/V702/lib/libpgcommon.a $(BUILD_DIR)/walproposer-lib
	$(AR) d $(BUILD_DIR)/walproposer-lib/libpgport.a \
		pg_strong_random.o
	$(AR) d $(BUILD_DIR)/walproposer-lib/libpgcommon.a \
		checksum_helper.o \
		cryptohash_openssl.o \
		hmac_openssl.o \
		md5_common.o \
		parse_manifest.o \
		scram-common.o
ifeq ($(UNAME_S),Linux)
	$(AR) d $(BUILD_DIR)/walproposer-lib/libpgcommon.a \
		pg_crc32c.o
endif

# Shorthand to call neon-pg-ext-% target for all openGauss versions
.PHONY: neon-pg-ext
neon-pg-ext: $(foreach pg_version,$(OPENGAUSS_VERSIONS),neon-pg-ext-$(pg_version))

.PHONY: configure-release configure-debug
configure-release:
	@[ -f .neon/config ] && sed -i 's|target/debug|target/release|g' .neon/config || true

configure-debug:
	@[ -f .neon/config ] && sed -i 's|target/release|target/debug|g' .neon/config || true

# This removes everything
.PHONY: distclean
distclean:
	$(RM) -r $(BUILD_DIR) $(OPENGAUSS_INSTALL_DIR)
	$(CARGO_CMD_PREFIX) cargo clean

.PHONY: fmt
fmt:
	./pre-commit.py --fix-inplace

.PHONY: setup-pre-commit-hook
setup-pre-commit-hook:
	ln -s -f $(ROOT_PROJECT_DIR)/pre-commit.py .git/hooks/pre-commit

build-tools/node_modules: build-tools/package.json
	cd build-tools && $(if $(CI),npm ci,npm install)
	touch build-tools/node_modules

.PHONY: lint-openapi-spec
lint-openapi-spec: build-tools/node_modules
	# operation-2xx-response: pageserver timeline delete returns 404 on success
	find . -iname "openapi_spec.y*ml" -exec\
		npx --prefix=build-tools/ redocly\
			--skip-rule=operation-operationId --skip-rule=operation-summary --extends=minimal\
			--skip-rule=no-server-example.com --skip-rule=operation-2xx-response\
			lint {} \+


ifdef OG_INSTALL_CACHED
opengauss-build-%: 
	+@echo "Skipping openGauss installation because OG_INSTALL_CACHED is set"
else
# Targets for building openGauss are defined in opengauss.mk.
include opengauss.mk
endif

