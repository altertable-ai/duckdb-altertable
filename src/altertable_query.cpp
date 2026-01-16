#include "altertable_scanner.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "storage/altertable_catalog.hpp"
#include "storage/altertable_transaction.hpp"
#include "arrow/type.h"
#include "arrow/api.h"

namespace duckdb {

static LogicalType GetArrowLogicalType(const arrow::DataType &arrow_type) {
	switch (arrow_type.id()) {
	case arrow::Type::BOOL:
		return LogicalType::BOOLEAN;
	case arrow::Type::INT8:
		return LogicalType::TINYINT;
	case arrow::Type::INT16:
		return LogicalType::SMALLINT;
	case arrow::Type::INT32:
		return LogicalType::INTEGER;
	case arrow::Type::INT64:
		return LogicalType::BIGINT;
	case arrow::Type::UINT8:
		return LogicalType::UTINYINT;
	case arrow::Type::UINT16:
		return LogicalType::USMALLINT;
	case arrow::Type::UINT32:
		return LogicalType::UINTEGER;
	case arrow::Type::UINT64:
		return LogicalType::UBIGINT;
	case arrow::Type::FLOAT:
		return LogicalType::FLOAT;
	case arrow::Type::DOUBLE:
		return LogicalType::DOUBLE;
	case arrow::Type::STRING:
	case arrow::Type::LARGE_STRING:
		return LogicalType::VARCHAR;
	case arrow::Type::BINARY:
	case arrow::Type::LARGE_BINARY:
		return LogicalType::BLOB;
	case arrow::Type::DATE32:
		return LogicalType::DATE;
	case arrow::Type::TIMESTAMP:
		return LogicalType::TIMESTAMP;
	case arrow::Type::DECIMAL:
		return LogicalType::DECIMAL(18, 3); // Approximate
	default:
		return LogicalType::VARCHAR; // Fallback
	}
}

static unique_ptr<FunctionData> PGQueryBind(ClientContext &context, TableFunctionBindInput &input,
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

	// Execute the query to get schema from FlightInfo
	auto info = con.Execute(query);

	// Get Schema
	std::shared_ptr<arrow::Schema> schema;
	arrow::ipc::DictionaryMemo memo;
	auto schema_result = info->GetSchema(&memo);
	if (!schema_result.ok()) {
		throw IOException("Failed to get schema from FlightInfo: " + schema_result.status().ToString());
	}
	schema = schema_result.ValueOrDie();

	for (int i = 0; i < schema->num_fields(); i++) {
		auto field = schema->field(i);
		names.push_back(field->name());
		return_types.push_back(GetArrowLogicalType(*field->type()));
	}

	bind_data->names = names;
	bind_data->types = return_types;

	return std::move(bind_data);
}

AltertableQueryFunction::AltertableQueryFunction()
    : TableFunction("altertable_query", {LogicalType::VARCHAR, LogicalType::VARCHAR}, nullptr, PGQueryBind) {
	named_parameters["use_transaction"] = LogicalType::BOOLEAN;
	AltertableScanFunction scan_function;
	init_global = scan_function.init_global;
	init_local = scan_function.init_local;
	function = scan_function.function;
	projection_pushdown = true;
	global_initialization = TableFunctionInitialization::INITIALIZE_ON_SCHEDULE;
}
} // namespace duckdb
