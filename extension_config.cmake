# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(altertable
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
)
# Link the extension into the DuckDB targets (shell, unittest) so tests can `require altertable`
duckdb_extension_statically_link(altertable)

# Any extra extensions that should be built
# e.g.: duckdb_extension_load(json)
