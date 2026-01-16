//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/altertable_table_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "altertable_utils.hpp"

namespace duckdb {

struct AltertableTableInfo {
	AltertableTableInfo() {
		create_info = make_uniq<CreateTableInfo>();
		create_info->columns.SetAllowDuplicates(true);
	}
	AltertableTableInfo(const string &schema, const string &table) {
		create_info = make_uniq<CreateTableInfo>(string(), schema, table);
		create_info->columns.SetAllowDuplicates(true);
	}
	AltertableTableInfo(const SchemaCatalogEntry &schema, const string &table) {
		create_info = make_uniq<CreateTableInfo>((SchemaCatalogEntry &)schema, table);
		create_info->columns.SetAllowDuplicates(true);
	}

	const string &GetTableName() const {
		return create_info->table;
	}

	unique_ptr<CreateTableInfo> create_info;
	vector<AltertableType> altertable_types;
	vector<string> altertable_names;
	idx_t approx_num_pages = 0;
};

class AltertableTableEntry : public TableCatalogEntry {
public:
	AltertableTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info);
	AltertableTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, AltertableTableInfo &info);

public:
	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;

	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;

	TableStorageInfo GetStorageInfo(ClientContext &context) override;

	void BindUpdateConstraints(Binder &binder, LogicalGet &get, LogicalProjection &proj, LogicalUpdate &update,
	                           ClientContext &context) override;

public:
	//! Altertable type annotations
	vector<AltertableType> altertable_types;
	//! Column names as they are within Altertable
	//! We track these separately because of case sensitivity - Altertable allows e.g. the columns "ID" and "id"
	//! together We would in this case remap them to "ID" and "id:1", while altertable_names store the original names
	vector<string> altertable_names;
	//! The approximate number of pages a table consumes in Altertable
	idx_t approx_num_pages;
};

} // namespace duckdb
