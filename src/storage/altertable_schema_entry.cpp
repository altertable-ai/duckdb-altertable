#include "storage/altertable_schema_entry.hpp"
#include "storage/altertable_transaction.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/constraints/list.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"

namespace duckdb {

AltertableSchemaEntry::AltertableSchemaEntry(Catalog &catalog, CreateSchemaInfo &info)
    : SchemaCatalogEntry(catalog, info), tables(*this), indexes(*this) {
}

AltertableSchemaEntry::AltertableSchemaEntry(Catalog &catalog, CreateSchemaInfo &info,
                                             unique_ptr<AltertableResultSlice> tables,
                                             unique_ptr<AltertableResultSlice> indexes)
    : SchemaCatalogEntry(catalog, info), tables(*this, std::move(tables)), indexes(*this, std::move(indexes)) {
}

bool AltertableSchemaEntry::SchemaIsInternal(const string &name) {
	if (name == "information_schema" || StringUtil::StartsWith(name, "pg_")) {
		return true;
	}
	return false;
}

AltertableTransaction &GetAltertableTransaction(CatalogTransaction transaction) {
	if (!transaction.transaction) {
		throw InternalException("No transaction!?");
	}
	return transaction.transaction->Cast<AltertableTransaction>();
}

void AltertableSchemaEntry::TryDropEntry(ClientContext &context, CatalogType catalog_type, const string &name) {
	DropInfo info;
	info.type = catalog_type;
	info.name = name;
	info.cascade = false;
	info.if_not_found = OnEntryNotFound::RETURN_NULL;
	DropEntry(context, info);
}

optional_ptr<CatalogEntry> AltertableSchemaEntry::CreateTable(CatalogTransaction transaction,
                                                              BoundCreateTableInfo &info) {
	auto &altertable_transaction = GetAltertableTransaction(transaction);
	auto &base_info = info.Base();
	auto table_name = base_info.table;
	if (base_info.on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
		// CREATE OR REPLACE - drop any existing entries first (if any)
		TryDropEntry(transaction.GetContext(), CatalogType::TABLE_ENTRY, table_name);
	}
	return tables.CreateTable(altertable_transaction, info);
}

optional_ptr<CatalogEntry> AltertableSchemaEntry::CreateFunction(CatalogTransaction transaction,
                                                                 CreateFunctionInfo &info) {
	throw BinderException("Altertable databases do not support creating functions");
}

optional_ptr<CatalogEntry> AltertableSchemaEntry::CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
                                                              TableCatalogEntry &table) {
	throw BinderException("Altertable databases do not support creating indexes locally - use altertable_execute to "
	                      "create indexes on the remote server");
}

string GetCreateViewSQL(AltertableSchemaEntry &schema, CreateViewInfo &info) {
	string sql;
	sql = "CREATE VIEW ";
	sql += AltertableUtils::QuoteAltertableIdentifier(schema.name) + ".";
	sql += AltertableUtils::QuoteAltertableIdentifier(info.view_name);
	sql += " ";
	if (!info.aliases.empty()) {
		sql += "(";
		for (idx_t i = 0; i < info.aliases.size(); i++) {
			if (i > 0) {
				sql += ", ";
			}
			auto &alias = info.aliases[i];
			sql += AltertableUtils::QuoteAltertableIdentifier(alias);
		}
		sql += ") ";
	}
	sql += "AS ";
	sql += info.query->ToString();
	return sql;
}

optional_ptr<CatalogEntry> AltertableSchemaEntry::CreateView(CatalogTransaction transaction, CreateViewInfo &info) {
	if (info.sql.empty()) {
		throw BinderException("Cannot create view in Altertable that originated from an empty SQL statement");
	}
	if (info.on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT ||
	    info.on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
		auto current_entry = GetEntry(transaction, CatalogType::VIEW_ENTRY, info.view_name);
		if (current_entry) {
			if (info.on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
				return current_entry;
			}
			// CREATE OR REPLACE - drop any existing entries first (if any)
			TryDropEntry(transaction.GetContext(), CatalogType::VIEW_ENTRY, info.view_name);
		}
	}
	auto &altertable_transaction = GetAltertableTransaction(transaction);
	altertable_transaction.Query(GetCreateViewSQL(*this, info));
	return tables.ReloadEntry(altertable_transaction, info.view_name);
}

optional_ptr<CatalogEntry> AltertableSchemaEntry::CreateType(CatalogTransaction transaction, CreateTypeInfo &info) {
	throw BinderException("Altertable databases do not support creating types - use altertable_execute to create types "
	                      "on the remote server");
}

optional_ptr<CatalogEntry> AltertableSchemaEntry::CreateSequence(CatalogTransaction transaction,
                                                                 CreateSequenceInfo &info) {
	throw BinderException("Altertable databases do not support creating sequences");
}

optional_ptr<CatalogEntry> AltertableSchemaEntry::CreateTableFunction(CatalogTransaction transaction,
                                                                      CreateTableFunctionInfo &info) {
	throw BinderException("Altertable databases do not support creating table functions");
}

optional_ptr<CatalogEntry> AltertableSchemaEntry::CreateCopyFunction(CatalogTransaction transaction,
                                                                     CreateCopyFunctionInfo &info) {
	throw BinderException("Altertable databases do not support creating copy functions");
}

optional_ptr<CatalogEntry> AltertableSchemaEntry::CreatePragmaFunction(CatalogTransaction transaction,
                                                                       CreatePragmaFunctionInfo &info) {
	throw BinderException("Altertable databases do not support creating pragma functions");
}

optional_ptr<CatalogEntry> AltertableSchemaEntry::CreateCollation(CatalogTransaction transaction,
                                                                  CreateCollationInfo &info) {
	throw BinderException("Altertable databases do not support creating collations");
}

void AltertableSchemaEntry::Alter(CatalogTransaction transaction, AlterInfo &info) {
	if (info.type != AlterType::ALTER_TABLE) {
		throw BinderException("Only altering tables is supported for now");
	}
	auto &altertable_transaction = GetAltertableTransaction(transaction);
	auto &alter = info.Cast<AlterTableInfo>();
	tables.AlterTable(altertable_transaction, alter);
}

bool CatalogTypeIsSupported(CatalogType type) {
	switch (type) {
	case CatalogType::INDEX_ENTRY:
	case CatalogType::TABLE_ENTRY:
	case CatalogType::VIEW_ENTRY:
		return true;
	default:
		return false;
	}
}

void AltertableSchemaEntry::Scan(ClientContext &context, CatalogType type,
                                 const std::function<void(CatalogEntry &)> &callback) {
	if (!CatalogTypeIsSupported(type)) {
		return;
	}
	auto &altertable_transaction = AltertableTransaction::Get(context, catalog);
	GetCatalogSet(type).Scan(altertable_transaction, callback);
}
void AltertableSchemaEntry::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
	throw NotImplementedException("Scan without context not supported");
}

void AltertableSchemaEntry::DropEntry(ClientContext &context, DropInfo &info) {
	info.schema = name;
	auto &altertable_transaction = AltertableTransaction::Get(context, catalog);
	GetCatalogSet(info.type).DropEntry(altertable_transaction, info);
}

optional_ptr<CatalogEntry> AltertableSchemaEntry::LookupEntry(CatalogTransaction transaction,
                                                              const EntryLookupInfo &lookup_info) {
	auto catalog_type = lookup_info.GetCatalogType();
	if (!CatalogTypeIsSupported(catalog_type)) {
		return nullptr;
	}
	auto &altertable_transaction = GetAltertableTransaction(transaction);
	return GetCatalogSet(catalog_type).GetEntry(altertable_transaction, lookup_info.GetEntryName());
}

AltertableCatalogSet &AltertableSchemaEntry::GetCatalogSet(CatalogType type) {
	switch (type) {
	case CatalogType::TABLE_ENTRY:
	case CatalogType::VIEW_ENTRY:
		return tables;
	case CatalogType::INDEX_ENTRY:
		return indexes;
	default:
		throw InternalException("Type not supported for GetCatalogSet");
	}
}

} // namespace duckdb
