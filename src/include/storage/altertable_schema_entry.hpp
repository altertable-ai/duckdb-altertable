//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/altertable_schema_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "storage/altertable_table_set.hpp"
#include "storage/altertable_index_set.hpp"

namespace duckdb {
class AltertableTransaction;

class AltertableSchemaEntry : public SchemaCatalogEntry {
public:
	AltertableSchemaEntry(Catalog &catalog, CreateSchemaInfo &info);
	AltertableSchemaEntry(Catalog &catalog, CreateSchemaInfo &info, unique_ptr<AltertableResultSlice> tables,
	                    unique_ptr<AltertableResultSlice> indexes);

public:
	optional_ptr<CatalogEntry> CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) override;
	optional_ptr<CatalogEntry> CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
	                                       TableCatalogEntry &table) override;
	optional_ptr<CatalogEntry> CreateView(CatalogTransaction transaction, CreateViewInfo &info) override;
	optional_ptr<CatalogEntry> CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) override;
	optional_ptr<CatalogEntry> CreateTableFunction(CatalogTransaction transaction,
	                                               CreateTableFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateCopyFunction(CatalogTransaction transaction,
	                                              CreateCopyFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreatePragmaFunction(CatalogTransaction transaction,
	                                                CreatePragmaFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateCollation(CatalogTransaction transaction, CreateCollationInfo &info) override;
	optional_ptr<CatalogEntry> CreateType(CatalogTransaction transaction, CreateTypeInfo &info) override;
	void Alter(CatalogTransaction transaction, AlterInfo &info) override;
	void Scan(ClientContext &context, CatalogType type, const std::function<void(CatalogEntry &)> &callback) override;
	void Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) override;
	void DropEntry(ClientContext &context, DropInfo &info) override;
	optional_ptr<CatalogEntry> LookupEntry(CatalogTransaction transaction, const EntryLookupInfo &lookup_info) override;

	static bool SchemaIsInternal(const string &name);

private:
	void AlterTable(AltertableTransaction &transaction, RenameTableInfo &info);
	void AlterTable(AltertableTransaction &transaction, RenameColumnInfo &info);
	void AlterTable(AltertableTransaction &transaction, AddColumnInfo &info);
	void AlterTable(AltertableTransaction &transaction, RemoveColumnInfo &info);

	void TryDropEntry(ClientContext &context, CatalogType catalog_type, const string &name);

	AltertableCatalogSet &GetCatalogSet(CatalogType type);

private:
	AltertableTableSet tables;
	AltertableIndexSet indexes;
};

} // namespace duckdb
