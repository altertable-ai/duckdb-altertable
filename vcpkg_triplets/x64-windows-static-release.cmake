# vcpkg's community x64-windows-static-release triplet, plus /GF- (no string
# pooling) for every port.
#
# DuckDB v2.0's extension CI links extensions against a prebuilt
# duckdb_static.lib compiled with clang-cl, while vcpkg ports here are compiled
# with MSVC. Both pool identical string literals into same-named COMDATs, and
# link.exe may keep clang-cl's 1-byte-aligned copy. MSVC code then reads it
# with aligned SSE loads (movaps) and crashes before main(): Arrow's
# base64_chars initializer faults with 0xC0000005 in unittest and when loading
# the .duckdb_extension. With /GF-, MSVC keeps literals private to each object,
# so Arrow (and the other ports) always read their own, correctly aligned copy.
# See duckdb/duckdb#27857 and duckdb/extension-ci-tools#430.
#
# VCPKG_*_FLAGS_RELEASE come after /O2 (which implies /GF) on the command line,
# so /GF- wins.
set(VCPKG_TARGET_ARCHITECTURE x64)
set(VCPKG_CRT_LINKAGE static)
set(VCPKG_LIBRARY_LINKAGE static)

set(VCPKG_BUILD_TYPE release)

set(VCPKG_C_FLAGS_RELEASE "/GF-")
set(VCPKG_CXX_FLAGS_RELEASE "/GF-")
