# This file is included by DuckDB's build system. It specifies which extension to load

# On Windows, DuckDB v2.0's extension CI links against a prebuilt
# duckdb_static.lib compiled with clang-cl. Code compiled here with MSVC must not
# pool string literals with it: link.exe may keep clang-cl's 1-byte-aligned copy,
# and MSVC's aligned SSE loads then fault. vcpkg_triplets/ does the same for the
# vcpkg ports. See duckdb/duckdb#27857 and duckdb/extension-ci-tools#430.
if(MSVC AND CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")
    add_compile_options(/GF-)
endif()

# Extension from this repo
duckdb_extension_load(altertable
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
)
# Link the extension into the DuckDB targets (shell, unittest) so tests can `require altertable`
duckdb_extension_statically_link(altertable)

# Any extra extensions that should be built
# e.g.: duckdb_extension_load(json)
