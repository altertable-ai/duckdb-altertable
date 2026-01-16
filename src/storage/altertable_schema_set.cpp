//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/altertable_schema_set.cpp
//
//
//===----------------------------------------------------------------------===//

#include "storage/altertable_schema_set.hpp"
#include "storage/altertable_index_set.hpp"
#include "storage/altertable_table_set.hpp"
#include "storage/altertable_transaction.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "storage/altertable_table_set.hpp"
#include "arrow/table.h"
#include "arrow/array.h"
#include "storage/altertable_catalog.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "arrow/flight/sql/client.h"

namespace duckdb {

AltertableSchemaSet::AltertableSchemaSet(Catalog &catalog, string schema_to_load_p)
    : AltertableCatalogSet(catalog, false), schema_to_load(std::move(schema_to_load_p)) {
}

void AltertableSchemaSet::LoadEntries(AltertableTransaction &transaction) {
	auto *client = transaction.GetConnection().GetClient();
	auto &call_options = transaction.GetConnection().GetCallOptions();
	auto &altertable_catalog = catalog.Cast<AltertableCatalog>();
	auto &remote_catalog = altertable_catalog.GetRemoteCatalog();

	// Flight SQL GetDbSchemas - filter by catalog if one is set
	std::optional<std::string> catalog_filter;
	if (!remote_catalog.empty()) {
		catalog_filter = remote_catalog;
	}
	auto info_result = client->GetDbSchemas(call_options, catalog_filter ? &*catalog_filter : nullptr, nullptr);
	if (!info_result.ok()) {
		throw IOException("Failed to GetDbSchemas: " + info_result.status().ToString());
	}

	auto stream_result = client->DoGet(call_options, info_result.ValueOrDie()->endpoints()[0].ticket);
	if (!stream_result.ok()) {
		throw IOException("Failed to stream schemas: " + stream_result.status().ToString());
	}

	auto reader = std::move(stream_result.ValueOrDie());
	auto table_chunk = reader->ToTable();
	if (!table_chunk.ok()) {
		throw IOException("Failed to read schema table: " + table_chunk.status().ToString());
	}

	auto table = table_chunk.ValueOrDie();
	// Expected columns: catalog_name, db_schema_name

	auto schema_col = table->GetColumnByName("db_schema_name");
	auto catalog_col = table->GetColumnByName("catalog_name");
	if (!schema_col) {
		// Fallback or error?
		return;
	}

	auto schemas = std::static_pointer_cast<arrow::StringArray>(schema_col->chunk(0));
	std::shared_ptr<arrow::StringArray> catalogs;
	if (catalog_col) {
		catalogs = std::static_pointer_cast<arrow::StringArray>(catalog_col->chunk(0));
	}

	for (int64_t i = 0; i < schemas->length(); ++i) {
		// If we have a remote catalog filter and the row's catalog doesn't match, skip it
		if (!remote_catalog.empty() && catalogs && catalogs->IsValid(i)) {
			string row_catalog = catalogs->GetString(i);
			if (row_catalog != remote_catalog) {
				continue;
			}
		}

		string schema_name = schemas->GetString(i);

		CreateSchemaInfo info;
		info.schema = schema_name;
		info.internal = AltertableSchemaEntry::SchemaIsInternal(schema_name);

		// We pass empty slices for now as we'll load tables lazily or in a different way
		auto schema = make_shared_ptr<AltertableSchemaEntry>(catalog, info, nullptr, nullptr);

		CreateEntry(transaction, std::move(schema));
	}
}

optional_ptr<CatalogEntry> AltertableSchemaSet::CreateSchema(AltertableTransaction &transaction,
                                                             CreateSchemaInfo &info) {
	throw NotImplementedException("CreateSchema not supported in Flight SQL yet");
}

} // namespace duckdb
