# openGauss build targets (included from main Makefile)

OPENGAUSS_SRC := $(ROOT_PROJECT_DIR)/vendor/openGauss

# Main targets
opengauss-install-%: opengauss-build-%
	@echo "openGauss $* installation completed"

opengauss-headers-install-%: opengauss-build-%
	@echo "openGauss $* headers available at $(OPENGAUSS_INSTALL_DIR)/$*/include"

opengauss-check-%: opengauss-install-%
	$(MAKE) -C $(BUILD_DIR)/$* MAKELEVEL=0 check

# Shorthands
.PHONY: opengauss opengauss-headers opengauss-install opengauss-headers-install opengauss-check
opengauss: opengauss-install
opengauss-headers: opengauss-headers-install
opengauss-install opengauss-headers-install opengauss-check: opengauss-%: $(foreach v,$(OPENGAUSS_VERSIONS),opengauss-%-$(v))
$(foreach v,$(OPENGAUSS_VERSIONS),opengauss-$(v)): opengauss-%: opengauss-install-%

# Build target
opengauss-build-%: FORCE
	@echo "Building openGauss $*"
	@test -s $(OPENGAUSS_SRC)/build.sh || { \
		echo "openGauss submodule not found. Run: git submodule update --init --recursive"; exit 1; }
	@cd $(OPENGAUSS_SRC) && sh build.sh -m release -3rd $(OPENGAUSS_BINARYLIBS_DIR) --cmake --cmake_opt "-DENABLE_NEON=ON"
	@rm -rf $(OPENGAUSS_INSTALL_DIR)/$* && mkdir -p $(OPENGAUSS_INSTALL_DIR)/$*
	@test -d $(OPENGAUSS_SRC)/mppdb_temp_install && \
		cp -r $(OPENGAUSS_SRC)/mppdb_temp_install/* $(OPENGAUSS_INSTALL_DIR)/$*/ || \
		{ echo "Error: mppdb_temp_install not found"; exit 1; }
	@echo "Installed to $(OPENGAUSS_INSTALL_DIR)/$*"

.PHONY: FORCE
FORCE:
