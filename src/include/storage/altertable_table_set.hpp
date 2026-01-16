//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/altertable_table_set.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/altertable_catalog_set.hpp"
#include "storage/altertable_table_entry.hpp"

namespace duckdb {
struct CreateTableInfo;
class AltertableConnection;
class AltertableResult;
class AltertableSchemaEntry;

class AltertableTableSet : public AltertableInSchemaSet {
public:
	~AltertableTableSet() override = default;
	explicit AltertableTableSet(AltertableSchemaEntry &schema, unique_ptr<AltertableResultSlice> tables = nullptr);

public:
	optional_ptr<CatalogEntry> CreateTable(AltertableTransaction &transaction, BoundCreateTableInfo &info);

	static unique_ptr<AltertableTableInfo> GetTableInfo(AltertableTransaction &transaction,
	                                                    AltertableSchemaEntry &schema, const string &table_name);
	static unique_ptr<AltertableTableInfo> GetTableInfo(AltertableConnection &connection, const string &schema_name,
	                                                    const string &table_name);
	optional_ptr<CatalogEntry> ReloadEntry(AltertableTransaction &transaction, const string &table_name) override;

	void AlterTable(AltertableTransaction &transaction, AlterTableInfo &info);

	static string GetInitializeQuery(const string &catalog = string(), const string &schema = string(),
	                                 const string &table = string());

protected:
	void LoadEntries(AltertableTransaction &transaction) override;
	bool SupportReload() const override {
		return true;
	}

	void AlterTable(AltertableTransaction &transaction, RenameTableInfo &info);
	void AlterTable(AltertableTransaction &transaction, RenameColumnInfo &info);
	void AlterTable(AltertableTransaction &transaction, AddColumnInfo &info);
	void AlterTable(AltertableTransaction &transaction, RemoveColumnInfo &info);

	static void AddColumn(optional_ptr<AltertableTransaction> transaction, optional_ptr<AltertableSchemaEntry> schema,
	                      AltertableResult &result, idx_t row, AltertableTableInfo &table_info);
	static void AddConstraint(AltertableResult &result, idx_t row, AltertableTableInfo &table_info);
	static void AddColumnOrConstraint(optional_ptr<AltertableTransaction> transaction,
	                                  optional_ptr<AltertableSchemaEntry> schema, AltertableResult &result, idx_t row,
	                                  AltertableTableInfo &table_info);

	void CreateEntries(AltertableTransaction &transaction, AltertableResult &result, idx_t start, idx_t end);

private:
	string GetAlterTablePrefix(AltertableTransaction &transaction, const string &name);
	string GetAlterTablePrefix(const string &name, optional_ptr<CatalogEntry> entry);
	string GetAlterTableColumnName(const string &name, optional_ptr<CatalogEntry> entry);

protected:
	unique_ptr<AltertableResultSlice> table_result;
};

} // namespace duckdb
