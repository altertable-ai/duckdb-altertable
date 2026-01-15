//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/altertable_schema_set.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/altertable_catalog_set.hpp"
#include "storage/altertable_schema_entry.hpp"

namespace duckdb {
struct CreateSchemaInfo;

class AltertableSchemaSet : public AltertableCatalogSet {
public:
	explicit AltertableSchemaSet(Catalog &catalog, string schema_to_load);

public:
	optional_ptr<CatalogEntry> CreateSchema(AltertableTransaction &transaction, CreateSchemaInfo &info);

	static string GetInitializeQuery(const string &schema = string());

protected:
	void LoadEntries(AltertableTransaction &transaction) override;

protected:
	//! Schema to load - if empty loads all schemas (default behavior)
	string schema_to_load;
};

} // namespace duckdb
