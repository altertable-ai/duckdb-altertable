//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/altertable_index_set.hpp
//
// Index set - Altertable manages indexes server-side, not exposed via catalog
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/altertable_catalog_set.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"

namespace duckdb {
class AltertableSchemaEntry;
class TableCatalogEntry;

class AltertableIndexSet : public AltertableInSchemaSet {
public:
	~AltertableIndexSet() override = default;
	explicit AltertableIndexSet(AltertableSchemaEntry &schema,
	                            unique_ptr<AltertableResultSlice> index_result = nullptr);

public:
	static string GetInitializeQuery(const string &schema = string());

	optional_ptr<CatalogEntry> CreateIndex(AltertableTransaction &transaction, CreateIndexInfo &info,
	                                       TableCatalogEntry &table);

protected:
	void LoadEntries(AltertableTransaction &transaction) override;

protected:
	unique_ptr<AltertableResultSlice> index_result;
};

} // namespace duckdb
