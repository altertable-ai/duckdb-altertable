//===----------------------------------------------------------------------===//
//                         DuckDB
//
// altertable_storage.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/storage_extension.hpp"

namespace duckdb {

class AltertableStorageExtension : public StorageExtension {
public:
	AltertableStorageExtension();
};

} // namespace duckdb
