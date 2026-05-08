PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=altertable
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

# GCC 14 (e.g. manylinux/RHEL toolchains) reports duplicate symbols for
# duckdb::BufferedFileWriter::DEFAULT_OPEN_FLAGS when linking tools/plan_serializer
# against libduckdb_static.a (constexpr + out-of-line definition in DuckDB).
# plan_serializer is only built when BUILD_SHELL is on; skip shell + plan_serializer.
# Set SKIP_DUCKDB_SHELL=0 if you need the duckdb CLI (and accept possible link failure on GCC 14).
SKIP_DUCKDB_SHELL ?= 1
ifeq ($(SKIP_DUCKDB_SHELL),1)
EXT_FLAGS += -DBUILD_SHELL=OFF
endif

ifeq ($(shell uname -s),Darwin)
	MACOS_SDK_PATH := $(shell xcrun --show-sdk-path 2>/dev/null)
ifneq ($(MACOS_SDK_PATH),)
	TIDY_PLATFORM_ARGS := -extra-arg=-isystem$(MACOS_SDK_PATH)/usr/include/c++/v1 -extra-arg=-isysroot -extra-arg=$(MACOS_SDK_PATH)
endif
endif

# tidy-check must configure with the vcpkg manifest (Arrow) like release/debug; upstream omits VCPKG_MANIFEST_FLAGS.
tidy-check: ${EXTENSION_CONFIG_STEP}
	mkdir -p ./build/tidy
	cmake $(GENERATOR) $(BUILD_FLAGS) $(EXT_DEBUG_FLAGS) $(VCPKG_MANIFEST_FLAGS) -DDISABLE_UNITY=1 -DCLANG_TIDY=1 -S $(DUCKDB_SRCDIR) -B build/tidy
	cp $(PROJ_DIR).clang-tidy build/tidy/.clang-tidy
	cd build/tidy && python3 ../../duckdb/scripts/run-clang-tidy.py '$(PROJ_DIR)src/.*/' -header-filter '$(PROJ_DIR)src/.*/' -quiet ${TIDY_THREAD_PARAMETER} ${TIDY_BINARY_PARAMETER} ${TIDY_PERFORM_CHECKS} ${TIDY_PLATFORM_ARGS}

# Run tests with the altertable-mock Docker container
test-mock:
	./scripts/test_with_mock.sh

# Generate compile_commands.json for clangd
clangd:
	mkdir -p build/clangd
	cmake $(GENERATOR) $(BUILD_FLAGS) $(EXT_DEBUG_FLAGS) $(VCPKG_MANIFEST_FLAGS) -DCMAKE_BUILD_TYPE=Debug -DCMAKE_EXPORT_COMPILE_COMMANDS=1 -S $(DUCKDB_SRCDIR) -B build/clangd
