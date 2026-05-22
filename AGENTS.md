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

Most integration tests in `./test/sql` use `require-env ALTERTABLE_TEST_*` and connect to a live Altertable server via `ATTACH ... TYPE ALTERTABLE`. Without those env vars set, sqllogictest **skips** them — so plain `make test` only runs a handful of offline tests (extension load, connection validation, etc.).

**Prefer mock-backed testing:**

- **`make test-mock`** — runs `./scripts/test_with_mock.sh`, which pulls and starts the official `altertable-mock` Docker container, exports `ALTERTABLE_TEST_HOST`, `ALTERTABLE_TEST_PORT`, `ALTERTABLE_TEST_USER`, `ALTERTABLE_TEST_PASSWORD`, and `ALTERTABLE_TEST_SSL`, then runs the full suite via `make test`. The container is stopped on exit. Requires Docker locally (CI uses a pre-started service instead).
- **`./scripts/test_with_mock.sh path/to/test_file.test`** — same mock setup, but runs a **single** test file via `./build/release/test/unittest` (pass the path relative to the repo root, e.g. `test/sql/aggregate_pushdown.test`).

Build first with `GEN=ninja make` before running either command.

**Manual / no Docker:** export the `ALTERTABLE_TEST_*` variables yourself (see `docs/README.md`) and run `make test` or `./build/release/test/unittest <path/to/test_file.test>`.
