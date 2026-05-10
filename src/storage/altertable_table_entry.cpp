#include "storage/altertable_catalog.hpp"
#include "storage/altertable_table_entry.hpp"
#include "storage/altertable_transaction.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "altertable_scanner.hpp"

namespace duckdb {

AltertableTableEntry::AltertableTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info)
    : TableCatalogEntry(catalog, schema, info) {
	for (idx_t c = 0; c < columns.LogicalColumnCount(); c++) {
		auto &col = columns.GetColumnMutable(LogicalIndex(c));
		if (col.GetType().HasAlias()) {
			col.TypeMutable() = AltertableUtils::RemoveAlias(col.GetType());
		}
		altertable_names.push_back(col.GetName());
	}
	approx_num_pages = 0;
}

AltertableTableEntry::AltertableTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, AltertableTableInfo &info)
    : TableCatalogEntry(catalog, schema, *info.create_info), altertable_names(std::move(info.altertable_names)) {
	approx_num_pages = info.approx_num_pages;
}

unique_ptr<BaseStatistics> AltertableTableEntry::GetStatistics(ClientContext &context, column_t column_id) {
	return nullptr;
}

void AltertableTableEntry::BindUpdateConstraints(Binder &binder, LogicalGet &, LogicalProjection &, LogicalUpdate &,
                                                 ClientContext &) {
}

TableFunction AltertableTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	auto &altertable_catalog = catalog.Cast<AltertableCatalog>();
	auto &transaction = Transaction::Get(context, catalog).Cast<AltertableTransaction>();

	auto result = make_uniq<AltertableBindData>(context);

	result->catalog_name = altertable_catalog.GetRemoteCatalog();
	result->schema_name = schema.name;
	result->table_name = name;
	result->dsn = transaction.GetDSN();
	result->attach_path = altertable_catalog.attach_path;
	result->SetCatalog(altertable_catalog);
	result->SetTable(*this);
	for (auto &col : columns.Logical()) {
		result->types.push_back(col.GetType());
	}
	result->names = altertable_names;
	AltertableScanFunction::PrepareBind(*result, approx_num_pages);

	bind_data = std::move(result);
	return AltertableScanFunction();
}

TableStorageInfo AltertableTableEntry::GetStorageInfo(ClientContext &context) {
	TableStorageInfo result;
	result.cardinality = 0;
	// get index info based on constraints
	for (auto &constraint : constraints) {
		if (constraint->type != ConstraintType::UNIQUE) {
			continue;
		}
		IndexInfo info;
		auto &unique = constraint->Cast<UniqueConstraint>();
		info.is_unique = true;
		info.is_primary = unique.IsPrimaryKey();
		info.is_foreign = false;
		if (unique.HasIndex()) {
			info.column_set.insert(unique.GetIndex().index);
		} else {
			for (auto &name : unique.GetColumnNames()) {
				info.column_set.insert(columns.GetColumn(name).Logical().index);
			}
		}
		result.index_info.push_back(std::move(info));
	}
	return result;
}

} // namespace duckdb
