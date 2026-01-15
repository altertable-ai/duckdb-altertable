add_library(
  altertable_ext_storage OBJECT
  altertable_catalog.cpp
  altertable_catalog_set.cpp
  altertable_connection_pool.cpp
  altertable_index_set.cpp
  altertable_schema_entry.cpp
  altertable_schema_set.cpp
  altertable_table_entry.cpp
  altertable_table_set.cpp
  altertable_transaction.cpp
  altertable_transaction_manager.cpp)
  
target_include_directories(altertable_ext_storage PRIVATE 
    ${Arrow_INCLUDE_DIRS} 
    ${ArrowFlight_INCLUDE_DIRS} 
    ${ArrowFlightSql_INCLUDE_DIRS})
    
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:altertable_ext_storage>
    PARENT_SCOPE)
