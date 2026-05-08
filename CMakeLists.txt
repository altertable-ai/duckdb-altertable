cmake_minimum_required(VERSION 3.20)
set(TARGET_NAME altertable)
project(${TARGET_NAME})

set(CMAKE_CXX_STANDARD "17" CACHE STRING "C++ standard to enforce")
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include_directories(src/include)

# Arrow Flight SQL requires C++17
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_definitions(-DFRONTEND=1 -D_GNU_SOURCE=1)
add_definitions(-DUSE_OPENSSL=1 -DHAVE_BIO_GET_DATA=1 -DHAVE_BIO_METH_NEW=1)
set(OPENSSL_USE_STATIC_LIBS TRUE)

find_package(OpenSSL REQUIRED)
find_package(Arrow REQUIRED CONFIG)
find_package(ArrowFlight REQUIRED CONFIG)
find_package(ArrowFlightSql REQUIRED CONFIG)

if(NOT MSVC)
  set(ALTERTABLE_EXTRA_CFLAGS
      "-Wno-pedantic -Wno-sign-compare -Wno-unused-variable")
endif()

set(CMAKE_CXX_FLAGS_DEBUG
    "${CMAKE_CXX_FLAGS_DEBUG} ${ALTERTABLE_EXTRA_CFLAGS}")
set(CMAKE_CXX_FLAGS_RELEASE
    "${CMAKE_CXX_FLAGS_RELEASE} ${ALTERTABLE_EXTRA_CFLAGS}")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO
    "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} ${ALTERTABLE_EXTRA_CFLAGS}")

set(CMAKE_C_FLAGS_DEBUG
    "${CMAKE_C_FLAGS_DEBUG} ${ALTERTABLE_EXTRA_CFLAGS}")
set(CMAKE_C_FLAGS_RELEASE
    "${CMAKE_C_FLAGS_RELEASE} ${ALTERTABLE_EXTRA_CFLAGS}")
set(CMAKE_C_FLAGS_RELWITHDEBINFO
    "${CMAKE_C_FLAGS_RELWITHDEBINFO} ${ALTERTABLE_EXTRA_CFLAGS}")

# Prefer shared Arrow libs; fall back to static (vcpkg static triplets)
if(TARGET Arrow::arrow_shared)
  set(ARROW_LIB Arrow::arrow_shared)
else()
  set(ARROW_LIB Arrow::arrow_static)
endif()
if(TARGET ArrowFlight::arrow_flight_shared)
  set(ARROW_FLIGHT_LIB ArrowFlight::arrow_flight_shared)
else()
  set(ARROW_FLIGHT_LIB ArrowFlight::arrow_flight_static)
endif()
if(TARGET ArrowFlightSql::arrow_flight_sql_shared)
  set(ARROW_FLIGHT_SQL_LIB ArrowFlightSql::arrow_flight_sql_shared)
else()
  set(ARROW_FLIGHT_SQL_LIB ArrowFlightSql::arrow_flight_sql_static)
endif()

get_target_property(ARROW_INCLUDE_DIRS ${ARROW_LIB} INTERFACE_INCLUDE_DIRECTORIES)
get_target_property(ARROW_FLIGHT_INCLUDE_DIRS ${ARROW_FLIGHT_LIB} INTERFACE_INCLUDE_DIRECTORIES)
get_target_property(ARROW_FLIGHT_SQL_INCLUDE_DIRS ${ARROW_FLIGHT_SQL_LIB} INTERFACE_INCLUDE_DIRECTORIES)

include_directories(
    include 
    src/include 
    ${OPENSSL_INCLUDE_DIR}
    ${ARROW_INCLUDE_DIRS}
    ${ARROW_FLIGHT_INCLUDE_DIRS}
    ${ARROW_FLIGHT_SQL_INCLUDE_DIRS}
)

add_subdirectory(src)

set(PARAMETERS "-no-warnings")
build_static_extension(${TARGET_NAME} ${ALL_OBJECT_FILES})
build_loadable_extension(${TARGET_NAME} ${PARAMETERS} ${ALL_OBJECT_FILES})

target_include_directories(
  ${TARGET_NAME}_extension
  PRIVATE include src/include ${OPENSSL_INCLUDE_DIR}
  ${ARROW_INCLUDE_DIRS}
  ${ARROW_FLIGHT_INCLUDE_DIRS}
  ${ARROW_FLIGHT_SQL_INCLUDE_DIRS}
)

target_include_directories(
  ${TARGET_NAME}_loadable_extension
  PRIVATE include src/include ${OPENSSL_INCLUDE_DIR}
  ${ARROW_INCLUDE_DIRS}
  ${ARROW_FLIGHT_INCLUDE_DIRS}
  ${ARROW_FLIGHT_SQL_INCLUDE_DIRS}
)

target_link_libraries(${TARGET_NAME}_extension
    ${OPENSSL_LIBRARIES}
    ${ARROW_LIB}
    ${ARROW_FLIGHT_LIB}
    ${ARROW_FLIGHT_SQL_LIB}
)

target_link_libraries(${TARGET_NAME}_loadable_extension
    ${OPENSSL_LIBRARIES}
    ${ARROW_LIB}
    ${ARROW_FLIGHT_LIB}
    ${ARROW_FLIGHT_SQL_LIB}
)

# Override the C++ standard for this extension to C++17
set_target_properties(${TARGET_NAME}_loadable_extension PROPERTIES
    CXX_STANDARD 17
    CXX_STANDARD_REQUIRED ON
    C_STANDARD 99
)

set_target_properties(${TARGET_NAME}_extension PROPERTIES
    CXX_STANDARD 17
    CXX_STANDARD_REQUIRED ON
    C_STANDARD 99
)

if(WIN32)
  target_link_libraries(${TARGET_NAME}_extension wsock32 ws2_32
                        wldap32 secur32 crypt32)
  target_link_libraries(${TARGET_NAME}_loadable_extension wsock32 ws2_32
                        wldap32 secur32 crypt32)
endif()
