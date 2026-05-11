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
#include "altertable_utils.hpp"
#include "altertable_physical.hpp"
#include "duckdb/execution/operator/scan/physical_column_data_scan.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/storage/database_size.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"

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
	return AltertableConnectionConfig::Parse(connection_string).catalog;
}

static string QuoteDSNValue(const string &value) {
	bool needs_quoting = value.empty();
	for (char c : value) {
		if (StringUtil::CharacterIsSpace(c) || c == '\'' || c == '"' || c == '\\' || c == '=') {
			needs_quoting = true;
			break;
		}
	}
	if (!needs_quoting) {
		return value;
	}
	string result = "'";
	for (char c : value) {
		if (c == '\'' || c == '\\') {
			result += '\\';
		}
		result += c;
	}
	result += "'";
	return result;
}

string AddConnectionOption(const KeyValueSecret &kv_secret, const string &name) {
	Value input_val = kv_secret.TryGetValue(name);
	if (input_val.IsNull()) {
		return string();
	}
	return name + "=" + QuoteDSNValue(input_val.ToString()) + " ";
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
	if (access_mode == AccessMode::READ_ONLY) {
		throw BinderException("Cannot create table in read-only Altertable database");
	}
	auto &insert =
	    planner.Make<AltertablePhysicalInsert>(op, *this, op.schema, std::move(op.info), op.estimated_cardinality);
	insert.children.push_back(plan);
	return insert;
}

static string AltertableInsertTarget(const AltertableCatalog &catalog, const AltertableTableEntry &table) {
	string result;
	if (!catalog.GetRemoteCatalog().empty()) {
		result += AltertableUtils::QuoteAltertableIdentifier(catalog.GetRemoteCatalog()) + ".";
	}
	result += AltertableUtils::QuoteAltertableIdentifier(table.schema.name) + ".";
	result += AltertableUtils::QuoteAltertableIdentifier(table.name);
	return result;
}

static string AltertableInsertColumnList(const vector<string> &column_names) {
	vector<string> columns;
	for (auto &name : column_names) {
		columns.push_back(AltertableUtils::QuoteAltertableIdentifier(name));
	}
	return "(" + StringUtil::Join(columns, ", ") + ")";
}

static bool BuildInsertColumnMapping(LogicalInsert &insert, AltertableTableEntry &target_table,
                                     idx_t source_column_count, vector<string> &column_names,
                                     vector<idx_t> &source_indices) {
	if (insert.column_index_map.empty()) {
		if (source_column_count != target_table.GetColumns().LogicalColumnCount()) {
			return false;
		}
		column_names = target_table.altertable_names;
		for (idx_t col = 0; col < column_names.size(); col++) {
			source_indices.push_back(col);
		}
		return true;
	}

	for (idx_t table_col = 0; table_col < insert.column_index_map.size(); table_col++) {
		auto source_idx = insert.column_index_map[PhysicalIndex(table_col)];
		if (source_idx == DConstants::INVALID_INDEX) {
			continue;
		}
		if (table_col >= target_table.altertable_names.size() || source_idx >= source_column_count) {
			return false;
		}
		column_names.push_back(target_table.altertable_names[table_col]);
		source_indices.push_back(source_idx);
	}
	return !column_names.empty();
}

static bool TryBuildValuesInsert(LogicalInsert &insert, PhysicalOperator &plan, AltertableCatalog &target_catalog,
                                 AltertableTableEntry &target_table, string &sql) {
	if (plan.type != PhysicalOperatorType::COLUMN_DATA_SCAN) {
		return false;
	}
	auto &scan = plan.Cast<PhysicalColumnDataScan>();
	if (!scan.collection || scan.collection->Count() == 0) {
		return false;
	}

	vector<string> column_names;
	vector<idx_t> source_indices;
	if (!BuildInsertColumnMapping(insert, target_table, scan.collection->ColumnCount(), column_names, source_indices)) {
		return false;
	}

	vector<string> values;
	for (auto &chunk : scan.collection->Chunks()) {
		for (idx_t row = 0; row < chunk.size(); row++) {
			vector<string> rendered_row;
			for (auto source_idx : source_indices) {
				rendered_row.push_back(chunk.GetValue(source_idx, row).ToSQLString());
			}
			values.push_back("(" + StringUtil::Join(rendered_row, ", ") + ")");
		}
	}
	if (values.empty()) {
		return false;
	}

	sql = "INSERT INTO " + AltertableInsertTarget(target_catalog, target_table) + " " +
	      AltertableInsertColumnList(column_names) + " VALUES " + StringUtil::Join(values, ", ");
	return true;
}

PhysicalOperator &AltertableCatalog::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner,
                                                LogicalInsert &op, optional_ptr<PhysicalOperator> plan) {
	if (access_mode == AccessMode::READ_ONLY) {
		throw BinderException("Cannot insert into read-only Altertable database");
	}
	if (!plan) {
		throw BinderException("INSERT DEFAULT VALUES is not supported for Altertable attached tables yet");
	}
	if (op.return_chunk) {
		throw BinderException("INSERT ... RETURNING is not supported for Altertable attached tables yet");
	}
	if (op.on_conflict_info.action_type != OnConflictAction::THROW) {
		throw BinderException("INSERT ON CONFLICT is not supported for Altertable attached tables yet");
	}

	auto &target_table = op.table.Cast<AltertableTableEntry>();
	string values_insert_sql;
	if (TryBuildValuesInsert(op, *plan, *this, target_table, values_insert_sql)) {
		return planner.Make<AltertablePhysicalExecuteUpdate>(*this, std::move(values_insert_sql),
		                                                     op.estimated_cardinality);
	}

	if (!op.column_index_map.empty()) {
		plan = planner.ResolveDefaultsProjection(op, *plan);
	}
	auto &insert = planner.Make<AltertablePhysicalInsert>(op.types, *this, op.table, op.estimated_cardinality);
	insert.children.push_back(*plan);
	return insert;
}

PhysicalOperator &AltertableCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner,
                                                LogicalDelete &op, PhysicalOperator &plan) {
	throw BinderException("DELETE from Altertable attached tables is not supported by DuckDB's row-id write path yet; "
	                      "use altertable_execute() to forward a remote DELETE statement");
}

PhysicalOperator &AltertableCatalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner,
                                                LogicalUpdate &op, PhysicalOperator &plan) {
	throw BinderException("UPDATE of Altertable attached tables is not supported by DuckDB's row-id write path yet; "
	                      "use altertable_execute() to forward a remote UPDATE statement");
}

} // namespace duckdb
