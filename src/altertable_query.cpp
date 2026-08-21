#include "altertable_scanner.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"
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
	optional_ptr<AltertableCatalog> attached_catalog;
	if (db && db->GetCatalog().GetCatalogType() == "altertable") {
		auto &altertable_catalog = db->GetCatalog().Cast<AltertableCatalog>();
		dsn = altertable_catalog.connection_string;
		attached_catalog = &altertable_catalog;
	}

	auto bind_data = make_uniq<AltertableBindData>(context);
	bind_data->dsn = dsn;
	bind_data->sql = query;
	bind_data->attach_path = dsn_or_db;
	if (attached_catalog) {
		bind_data->SetCatalog(*attached_catalog);
	}

	std::shared_ptr<arrow::Schema> schema;
	if (attached_catalog) {
		auto &transaction = Transaction::Get(context, *attached_catalog).Cast<AltertableTransaction>();
		schema = transaction.GetConnection().GetExecuteSchema(query);
	} else {
		auto con = AltertableConnection::Open(dsn);
		schema = con.GetExecuteSchema(query);
	}

	for (int i = 0; i < schema->num_fields(); i++) {
		auto field = schema->field(i);
		names.push_back(field->name());
		return_types.push_back(AltertableArrowTypeToLogicalType(*field->type()));
	}

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
	projection_pushdown = false;
	global_initialization = TableFunctionInitialization::INITIALIZE_ON_SCHEDULE;
}
} // namespace duckdb
