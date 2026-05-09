PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=altertable
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

# GCC / GNU ld (Linux, MinGW, Rtools): duplicate-definition for the constexpr
# static duckdb::BufferedFileWriter::DEFAULT_OPEN_FLAGS when linking
# plan_serializer against libduckdb_static.a. Allow the linker to merge them.
ifeq ($(shell uname -s),Linux)
EXT_FLAGS += '-DDUCKDB_EXTRA_LINK_FLAGS=-Wl,--allow-multiple-definition'
endif
ifeq ($(DUCKDB_PLATFORM),windows_amd64_mingw)
EXT_FLAGS += '-DDUCKDB_EXTRA_LINK_FLAGS=-Wl,--allow-multiple-definition'
endif
ifeq ($(DUCKDB_PLATFORM),windows_amd64_rtools)
EXT_FLAGS += '-DDUCKDB_EXTRA_LINK_FLAGS=-Wl,--allow-multiple-definition'
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
