#include "altertable_scanner.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/query_result.hpp"
#include "storage/altertable_catalog.hpp"
#include "storage/altertable_transaction.hpp"
#include "arrow/api.h"

namespace duckdb {

static unique_ptr<FunctionData> AltertableQueryBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
	auto dsn_or_db = input.inputs[0].GetValue<string>();
	auto query = input.inputs[1].GetValue<string>();
	string dsn = dsn_or_db;

	// Check if dsn_or_db is an attached database name
	auto &db_manager = DatabaseManager::Get(context);
	auto db = db_manager.GetDatabase(context, dsn_or_db);
	if (db && db->GetCatalog().GetCatalogType() == "altertable") {
		auto &altertable_catalog = db->GetCatalog().Cast<AltertableCatalog>();
		dsn = altertable_catalog.connection_string;
	}

	auto bind_data = make_uniq<AltertableBindData>(context);
	bind_data->dsn = dsn;
	bind_data->sql = query;
	bind_data->attach_path = dsn;
	// Note: we intentionally do NOT set the catalog here
	// This ensures the scanner uses fresh connections, avoiding transaction state issues

	// Open a fresh connection for schema discovery
	auto con = AltertableConnection::Open(dsn);

	auto schema = con.GetExecuteSchema(query);

	for (int i = 0; i < schema->num_fields(); i++) {
		auto field = schema->field(i);
		names.push_back(field->name());
		return_types.push_back(AltertableArrowTypeToLogicalType(*field->type()));
	}

	bind_data->names = names;
	bind_data->types = return_types;

	return std::move(bind_data);
}

static AltertableCatalog &GetAltertableCatalogByName(ClientContext &context, const string &db_name) {
	auto &db_manager = DatabaseManager::Get(context);
	auto db = db_manager.GetDatabase(context, db_name);
	if (!db) {
		throw BinderException("Database \"%s\" not found", db_name);
	}
	if (db->GetCatalog().GetCatalogType() != "altertable") {
		throw BinderException("Database \"%s\" is not an Altertable database", db_name);
	}
	return db->GetCatalog().Cast<AltertableCatalog>();
}

static unique_ptr<FunctionData> AltertableQueryByNameBind(ClientContext &context, TableFunctionBindInput &input,
                                                          vector<LogicalType> &return_types, vector<string> &names) {
	auto db_name = input.inputs[0].GetValue<string>();
	auto query = input.inputs[1].GetValue<string>();
	auto &altertable_catalog = GetAltertableCatalogByName(context, db_name);
	auto &transaction = AltertableTransaction::Get(context, altertable_catalog);
	auto &connection = transaction.GetConnection();

	auto bind_data = make_uniq<AltertableBindData>(context);
	bind_data->dsn = transaction.GetDSN();
	bind_data->sql = query;
	bind_data->attach_path = altertable_catalog.attach_path;
	bind_data->SetCatalog(altertable_catalog);

	auto schema = connection.GetExecuteSchema(query);
	for (int i = 0; i < schema->num_fields(); i++) {
		auto field = schema->field(i);
		names.push_back(field->name());
		return_types.push_back(AltertableArrowTypeToLogicalType(*field->type()));
	}
	QueryResult::DeduplicateColumns(names);

	bind_data->names = names;
	bind_data->types = return_types;

	return std::move(bind_data);
}

AltertableQueryFunction::AltertableQueryFunction()
    : TableFunction("altertable_query", {LogicalType::VARCHAR, LogicalType::VARCHAR}, nullptr, AltertableQueryBind) {
	AltertableScanFunction scan_function;
	init_global = scan_function.init_global;
	init_local = scan_function.init_local;
	function = scan_function.function;
	to_string = scan_function.to_string;
	projection_pushdown = false;
	global_initialization = TableFunctionInitialization::INITIALIZE_ON_SCHEDULE;
}

AltertableQueryByNameFunction::AltertableQueryByNameFunction()
    : TableFunction("altertable_query_by_name", {LogicalType::VARCHAR, LogicalType::VARCHAR}, nullptr,
                    AltertableQueryByNameBind) {
	AltertableScanFunction scan_function;
	init_global = scan_function.init_global;
	init_local = scan_function.init_local;
	function = scan_function.function;
	to_string = scan_function.to_string;
	projection_pushdown = false;
	global_initialization = TableFunctionInitialization::INITIALIZE_ON_SCHEDULE;
}
} // namespace duckdb
