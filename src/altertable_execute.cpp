//===----------------------------------------------------------------------===//
//                         DuckDB
//
// altertable_execute.cpp
//
// Execute DDL/DML statements on Altertable via Flight SQL
//===----------------------------------------------------------------------===//

#include "altertable_scanner.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "storage/altertable_catalog.hpp"
#include "storage/altertable_transaction.hpp"
#include "duckdb/common/enums/access_mode.hpp"

namespace duckdb {

struct AltertableExecuteBindData : public FunctionData {
	string dsn;
	string sql;

	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<AltertableExecuteBindData>();
		result->dsn = dsn;
		result->sql = sql;
		return std::move(result);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<AltertableExecuteBindData>();
		return dsn == other.dsn && sql == other.sql;
	}
};

struct AltertableExecuteGlobalState : public GlobalTableFunctionState {
	bool executed = false;
};

static unique_ptr<FunctionData> AltertableExecuteBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<AltertableExecuteBindData>();

	auto db_name = input.inputs[0].GetValue<string>();
	result->sql = input.inputs[1].GetValue<string>();

	// Check if db_name is an attached database
	auto &db_manager = DatabaseManager::Get(context);
	auto db = db_manager.GetDatabase(context, db_name);
	if (!db) {
		throw BinderException("Database \"%s\" not found", db_name);
	}
	if (db->GetCatalog().GetCatalogType() != "altertable") {
		throw BinderException("Database \"%s\" is not an Altertable database", db_name);
	}
	auto &altertable_catalog = db->GetCatalog().Cast<AltertableCatalog>();
	if (altertable_catalog.access_mode == AccessMode::READ_ONLY) {
		throw BinderException(
		    "Cannot use altertable_execute on read-only attached database \"%s\" (omit READ_ONLY or use read-write ATTACH)",
		    db_name);
	}
	result->dsn = altertable_catalog.connection_string;

	// Return a single row with success status
	names.push_back("success");
	return_types.push_back(LogicalType::BOOLEAN);

	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> AltertableExecuteInitGlobal(ClientContext &context,
                                                                        TableFunctionInitInput &input) {
	return make_uniq<AltertableExecuteGlobalState>();
}

static void AltertableExecuteFunc(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<AltertableExecuteBindData>();
	auto &gstate = data.global_state->Cast<AltertableExecuteGlobalState>();

	// Only execute once
	if (gstate.executed) {
		return;
	}
	gstate.executed = true;

	// Execute the statement with a fresh connection
	auto con = AltertableConnection::Open(bind_data.dsn);
	con.Query(bind_data.sql);

	output.SetCardinality(1);
	output.SetValue(0, 0, Value::BOOLEAN(true));
}

AltertableExecuteFunction::AltertableExecuteFunction()
    : TableFunction("altertable_execute", {LogicalType::VARCHAR, LogicalType::VARCHAR}, AltertableExecuteFunc,
                    AltertableExecuteBind, AltertableExecuteInitGlobal) {
}

} // namespace duckdb
