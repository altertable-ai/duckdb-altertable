include_directories(include ${Arrow_INCLUDE_DIRS} ${ArrowFlight_INCLUDE_DIRS}
                    ${ArrowFlightSql_INCLUDE_DIRS})

add_subdirectory(storage)

add_library(
  altertable_ext_library OBJECT
  altertable_connection.cpp
  altertable_execute.cpp
  altertable_extension.cpp
  altertable_optimizer.cpp
  altertable_query.cpp
  altertable_scanner.cpp
  altertable_storage.cpp
  altertable_utils.cpp)

target_include_directories(
  altertable_ext_library
  PRIVATE ${Arrow_INCLUDE_DIRS} ${ArrowFlight_INCLUDE_DIRS}
          ${ArrowFlightSql_INCLUDE_DIRS})

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:altertable_ext_library>
    PARENT_SCOPE)
