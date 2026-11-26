# Overlay of vcpkg's boost-locale port at 84bab45d (2025.12.12, the vcpkg_commit pinned in CI).
# Backports boostorg/locale f51b58cd34: CMake >= 4.5 rejects source_group(TREE ...) when a listed file
# (include/boost/locale.hpp) is outside the tree root, which breaks the macOS/Windows runners.
# Drop this overlay once the pinned vcpkg ships a Boost release containing that fix.

vcpkg_from_github(
    OUT_SOURCE_PATH SOURCE_PATH
    REPO boostorg/locale
    REF boost-${VERSION}
    SHA512 113e4a0abc158bbb0c6a2fae0b920449ecbd4346db8e3845f2c49bc1caca0497e15824e91546c91972d48976913860e23f51ee5700ce2c3f31c217fb8034fe1c
    HEAD_REF master
    PATCHES
        fix-cmake-4.5-source-group.patch
)

set(FEATURE_OPTIONS "")
include("${CMAKE_CURRENT_LIST_DIR}/features.cmake")
if(CMAKE_HOST_SYSTEM_NAME STREQUAL "Linux" AND VCPKG_TARGET_IS_MINGW)
    # mingw cross compile toolchain lacks std conv support
    list(APPEND FEATURE_OPTIONS "-DBOOST_LOCALE_ENABLE_STD=OFF")
endif()
boost_configure_and_install(
    SOURCE_PATH "${SOURCE_PATH}"
    OPTIONS ${FEATURE_OPTIONS}
)
