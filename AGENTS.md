# Altertable DuckDB extension

Instructions for AI agents working in this repository.

## Architecture

- This project is a DuckDB extension that provides access to the Altertable API
- It is built using CMake and Ninja
- It is tested using the sqllogictest framework
- It relies on a few submodules:
  - DuckDB (in `./duckdb`)
  - Extension CI Tools (in `./extension-ci-tools`)
- The project is organized into the following directories:
  - `./src`: Source code for the extension
  - `./include`: Header files for the extension
  - `./test`: Test files for the extension
  - `./docs`: Documentation for the extension
  - `./scripts`: Scripts for the extension

**IMPORTANT**: Never modify files in the `./duckdb` or `./extension-ci-tools` directories (and their children) unless explicitly instructed to do so.

## Coding style

- This project follows the DuckDB coding style
- You can use `make clangd` to generate a `compile_commands.json` file for clangd
- You can use `make format-check` to check the formatting
- You can use `make format-fix` to fix the formatting

## Build

- **ALWAYS use `GEN=ninja make` to compile this project**

## Debug

- **Debug builds are extremely slow** and should only be used in very rare use-cases

## Testing

- You can run the tests locally by running `make test` or by invoking the `./build/<release|debug>/test/unittest` binary directly
- You can run a single test with `./build/<release|debug>/test/unittest <path/to/test_file.test>`
