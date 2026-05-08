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
#include "altertable_utils.hpp"
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
	if (info_result.ValueOrDie()->endpoints().empty()) {
		throw IOException("Failed to stream schemas: GetDbSchemas returned no endpoints");
	}

	vector<std::shared_ptr<arrow::Table>> tables;
	for (auto &endpoint : info_result.ValueOrDie()->endpoints()) {
		auto stream_result = client->DoGet(call_options, endpoint.ticket);
		if (!stream_result.ok()) {
			throw IOException("Failed to stream schemas: " + stream_result.status().ToString());
		}

		auto reader = std::move(stream_result.ValueOrDie());
		auto table_chunk = reader->ToTable();
		if (!table_chunk.ok()) {
			throw IOException("Failed to read schema table: " + table_chunk.status().ToString());
		}
		tables.push_back(table_chunk.ValueOrDie());
	}
	std::shared_ptr<arrow::Table> table;
	if (tables.size() == 1) {
		table = std::move(tables[0]);
	} else {
		auto table_result = arrow::ConcatenateTables(tables);
		if (!table_result.ok()) {
			throw IOException("Failed to concatenate schema endpoint tables: " + table_result.status().ToString());
		}
		table = table_result.ValueOrDie();
	}
	// Expected columns: catalog_name, db_schema_name

	auto schema_col = table->GetColumnByName("db_schema_name");
	auto catalog_col = table->GetColumnByName("catalog_name");
	if (!schema_col) {
		throw IOException("Failed to load schemas: GetDbSchemas result did not include db_schema_name column");
	}

	for (int64_t chunk_idx = 0; chunk_idx < schema_col->num_chunks(); chunk_idx++) {
		auto schema_chunk = std::static_pointer_cast<arrow::StringArray>(schema_col->chunk(chunk_idx));
		std::shared_ptr<arrow::StringArray> catalog_chunk;
		if (catalog_col && chunk_idx < catalog_col->num_chunks()) {
			catalog_chunk = std::static_pointer_cast<arrow::StringArray>(catalog_col->chunk(chunk_idx));
		}

		for (int64_t i = 0; i < schema_chunk->length(); ++i) {
			// If we have a remote catalog filter and the row's catalog doesn't match, skip it
			if (!remote_catalog.empty() && catalog_chunk && catalog_chunk->IsValid(i)) {
				string row_catalog = catalog_chunk->GetString(i);
				if (row_catalog != remote_catalog) {
					continue;
				}
			}

			string schema_name = schema_chunk->GetString(i);
			if (!schema_to_load.empty() && schema_name != schema_to_load) {
				continue;
			}

			CreateSchemaInfo info;
			info.schema = schema_name;
			info.internal = AltertableSchemaEntry::SchemaIsInternal(schema_name);

			auto schema = make_shared_ptr<AltertableSchemaEntry>(catalog, info, nullptr, nullptr);
			CreateEntry(transaction, std::move(schema));
		}
	}
}

optional_ptr<CatalogEntry> AltertableSchemaSet::CreateSchema(AltertableTransaction &transaction,
                                                             CreateSchemaInfo &info) {
	string query = "CREATE SCHEMA ";
	if (info.on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
		query += "IF NOT EXISTS ";
	}
	query += AltertableUtils::QuoteAltertableIdentifier(info.schema);
	transaction.ExecuteUpdate(query);
	auto schema = make_shared_ptr<AltertableSchemaEntry>(catalog, info, nullptr, nullptr);
	return CreateEntry(transaction, std::move(schema));
}

} // namespace duckdb
