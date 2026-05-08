//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/altertable_catalog.cpp
//
//
//===----------------------------------------------------------------------===//

#include "storage/altertable_catalog.hpp"
#include "storage/altertable_schema_entry.hpp"
#include "storage/altertable_transaction.hpp"
#include "duckdb/storage/database_size.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

namespace duckdb {

AltertableCatalog::AltertableCatalog(AttachedDatabase &db_p, string connection_string_p, string attach_path_p,
                                     AccessMode access_mode, string schema_to_load)
    : Catalog(db_p), connection_string(std::move(connection_string_p)), attach_path(std::move(attach_path_p)),
      access_mode(access_mode), schemas(*this, schema_to_load), connection_pool(*this), default_schema(schema_to_load) {
	if (default_schema.empty()) {
		default_schema = "main";
	}
	remote_catalog = AltertableCatalog::ExtractCatalogFromConnectionString(connection_string);
}

string AltertableCatalog::ExtractCatalogFromConnectionString(const string &connection_string) {
	auto params = StringUtil::Split(connection_string, " ");
	string catalog;
	string legacy_catalog;
	for (const auto &param : params) {
		if (param.empty())
			continue;
		auto kv = StringUtil::Split(param, "=");
		if (kv.size() != 2) {
			continue;
		}
		auto key = StringUtil::Lower(kv[0]);
		if (key == "catalog") {
			catalog = kv[1];
		}
		if (key == "dbname" || key == "database") {
			legacy_catalog = kv[1];
		}
	}
	return catalog.empty() ? legacy_catalog : catalog;
}

string AddConnectionOption(const KeyValueSecret &kv_secret, const string &name) {
	Value input_val = kv_secret.TryGetValue(name);
	if (input_val.IsNull()) {
		// not provided
		return string();
	}
	string result;
	result += name;
	result += "=";
	result += input_val.ToString(); // No need to escape for DSN parsing in our simple parser
	result += " ";
	return result;
}

unique_ptr<SecretEntry> GetSecret(ClientContext &context, const string &secret_name) {
	auto &secret_manager = SecretManager::Get(context);
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
	// FIXME: this should be adjusted once the `GetSecretByName` API supports this use case
	auto secret_entry = secret_manager.GetSecretByName(transaction, secret_name, "memory");
	if (secret_entry) {
		return secret_entry;
	}
	secret_entry = secret_manager.GetSecretByName(transaction, secret_name, "local_file");
	if (secret_entry) {
		return secret_entry;
	}
	return nullptr;
}

string AltertableCatalog::GetConnectionString(ClientContext &context, const string &attach_path, string secret_name) {
	// if no secret is specified we default to the unnamed altertable secret, if it exists
	string connection_string = attach_path;
	bool explicit_secret = !secret_name.empty();
	if (!explicit_secret) {
		// look up settings from the default unnamed altertable secret if none is provided
		secret_name = "__default_altertable";
	}

	auto secret_entry = GetSecret(context, secret_name);
	if (secret_entry) {
		// secret found - read data
		auto kv_secret = dynamic_cast<const KeyValueSecret *>(secret_entry->secret.get());
		if (!kv_secret) {
			throw BinderException("Secret with name \"%s\" is not a key-value secret", secret_name);
		}
		string new_connection_info;

		new_connection_info += AddConnectionOption(*kv_secret, "user");
		new_connection_info += AddConnectionOption(*kv_secret, "password");
		new_connection_info += AddConnectionOption(*kv_secret, "host");
		new_connection_info += AddConnectionOption(*kv_secret, "port");
		new_connection_info += AddConnectionOption(*kv_secret, "ssl");
		new_connection_info += AddConnectionOption(*kv_secret, "catalog");
		new_connection_info += AddConnectionOption(*kv_secret, "dbname");
		new_connection_info += AddConnectionOption(*kv_secret, "database");

		connection_string = new_connection_info + connection_string;
	} else if (explicit_secret) {
		// secret not found and one was explicitly provided - throw an error
		throw BinderException("Secret with name \"%s\" not found", secret_name);
	}
	return connection_string;
}

AltertableCatalog::~AltertableCatalog() = default;

void AltertableCatalog::Initialize(bool load_builtin) {
}

optional_ptr<CatalogEntry> AltertableCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	auto &altertable_transaction = AltertableTransaction::Get(transaction.GetContext(), *this);
	auto entry = schemas.GetEntry(altertable_transaction, info.schema);
	if (entry) {
		switch (info.on_conflict) {
		case OnCreateConflict::REPLACE_ON_CONFLICT: {
			DropInfo try_drop;
			try_drop.type = CatalogType::SCHEMA_ENTRY;
			try_drop.name = info.schema;
			try_drop.if_not_found = OnEntryNotFound::RETURN_NULL;
			try_drop.cascade = false;
			schemas.DropEntry(altertable_transaction, try_drop);
			break;
		}
		case OnCreateConflict::IGNORE_ON_CONFLICT:
			return entry;
		case OnCreateConflict::ERROR_ON_CONFLICT:
		default:
			throw BinderException("Failed to create schema \"%s\": schema already exists", info.schema);
		}
	}
	return schemas.CreateSchema(altertable_transaction, info);
}

void AltertableCatalog::DropSchema(ClientContext &context, DropInfo &info) {
	auto &altertable_transaction = AltertableTransaction::Get(context, *this);
	return schemas.DropEntry(altertable_transaction, info);
}

void AltertableCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	auto &altertable_transaction = AltertableTransaction::Get(context, *this);
	schemas.Scan(altertable_transaction, [&](CatalogEntry &schema) { callback(schema.Cast<AltertableSchemaEntry>()); });
}

optional_ptr<SchemaCatalogEntry> AltertableCatalog::LookupSchema(CatalogTransaction transaction,
                                                                 const EntryLookupInfo &schema_lookup,
                                                                 OnEntryNotFound if_not_found) {
	auto schema_name = schema_lookup.GetEntryName();
	auto &altertable_transaction = AltertableTransaction::Get(transaction.GetContext(), *this);

	// In Flight SQL, we might not have the exact same "temporary schema" concept exposed in the same way,
	// but we'll keep the structure for now.

	auto entry = schemas.GetEntry(altertable_transaction, schema_name);
	if (!entry && if_not_found != OnEntryNotFound::RETURN_NULL) {
		throw BinderException("Schema with name \"%s\" not found", schema_name);
	}
	return reinterpret_cast<SchemaCatalogEntry *>(entry.get());
}

bool AltertableCatalog::InMemory() {
	return false;
}

string AltertableCatalog::GetDBPath() {
	return attach_path;
}

DatabaseSize AltertableCatalog::GetDatabaseSize(ClientContext &context) {
	// Flight SQL doesn't have a standard way to get database size.
	// We'll return 0 for now or implement a custom command if Altertable supports it.
	DatabaseSize size;
	size.free_blocks = 0;
	size.total_blocks = 0;
	size.used_blocks = 0;
	size.wal_size = 0;
	size.block_size = 0;
	size.bytes = 0;
	return size;
}

void AltertableCatalog::ClearCache() {
	schemas.ClearEntries();
}

// Altertable handles all compute server-side - these operations are not supported locally
// Use altertable_execute() to run DDL/DML statements on the remote server

PhysicalOperator &AltertableCatalog::PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                                       LogicalCreateTable &op, PhysicalOperator &plan) {
	throw NotImplementedException("CREATE TABLE AS not supported for Altertable - use altertable_execute() to create "
	                              "tables on the remote server");
}

PhysicalOperator &AltertableCatalog::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner,
                                                LogicalInsert &op, optional_ptr<PhysicalOperator> plan) {
	throw NotImplementedException(
	    "INSERT not supported for Altertable - use altertable_execute() to insert data on the remote server");
}

PhysicalOperator &AltertableCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner,
                                                LogicalDelete &op, PhysicalOperator &plan) {
	throw NotImplementedException(
	    "DELETE not supported for Altertable - use altertable_execute() to delete data on the remote server");
}

PhysicalOperator &AltertableCatalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner,
                                                LogicalUpdate &op, PhysicalOperator &plan) {
	throw NotImplementedException(
	    "UPDATE not supported for Altertable - use altertable_execute() to update data on the remote server");
}

} // namespace duckdb
