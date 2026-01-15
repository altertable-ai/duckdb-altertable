//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/altertable_index_set.cpp
//
// Index set implementation - Altertable doesn't expose indexes via catalog
//===----------------------------------------------------------------------===//

#include "storage/altertable_index_set.hpp"
#include "storage/altertable_schema_entry.hpp"
#include "storage/altertable_transaction.hpp"

namespace duckdb {

AltertableIndexSet::AltertableIndexSet(AltertableSchemaEntry &schema, unique_ptr<AltertableResultSlice> index_result_p)
    : AltertableInSchemaSet(schema, true), index_result(std::move(index_result_p)) {
	// Altertable doesn't expose indexes via catalog - they are managed server-side
}

string AltertableIndexSet::GetInitializeQuery(const string &schema) {
	// Not used - indexes are not exposed via Altertable catalog
	return "";
}

void AltertableIndexSet::LoadEntries(AltertableTransaction &transaction) {
	// No-op: Altertable manages indexes server-side
}

optional_ptr<CatalogEntry> AltertableIndexSet::CreateIndex(AltertableTransaction &transaction, CreateIndexInfo &info,
                                                         TableCatalogEntry &table) {
	throw BinderException("Creating indexes via catalog not supported - use altertable_execute to create indexes on the remote server");
}

} // namespace duckdb
