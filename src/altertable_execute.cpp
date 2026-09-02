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
	string db_name;
	string dsn;
	string sql;

	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<AltertableExecuteBindData>();
		result->db_name = db_name;
		result->dsn = dsn;
		result->sql = sql;
		return std::move(result);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<AltertableExecuteBindData>();
		return db_name == other.db_name && dsn == other.dsn && sql == other.sql;
	}
};

struct AltertableExecuteGlobalState : public GlobalTableFunctionState {
	bool executed = false;
	int64_t affected_rows = 0;
};

static unique_ptr<FunctionData> AltertableExecuteBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = make_uniq<AltertableExecuteBindData>();

	auto db_name = input.inputs[0].GetValue<string>();
	result->db_name = db_name;
	result->sql = input.inputs[1].GetValue<string>();

	// Check if db_name is an attached database
	auto &db_manager = DatabaseManager::Get(context);
	auto db = db_manager.GetDatabase(context, Identifier(db_name));
	if (!db) {
		throw BinderException("Database \"%s\" not found", db_name);
	}
	if (db->GetCatalog().GetCatalogType() != "altertable") {
		throw BinderException("Database \"%s\" is not an Altertable database", db_name);
	}
	auto &altertable_catalog = db->GetCatalog().Cast<AltertableCatalog>();
	if (altertable_catalog.access_mode == AccessMode::READ_ONLY) {
		throw BinderException("Cannot use altertable_execute on read-only attached database \"%s\" (omit READ_ONLY or "
		                      "use read-write ATTACH)",
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

	auto &db_manager = DatabaseManager::Get(context);
	auto db = db_manager.GetDatabase(context, Identifier(bind_data.db_name));
	if (!db || db->GetCatalog().GetCatalogType() != "altertable") {
		throw BinderException("Database \"%s\" is not an Altertable database", bind_data.db_name);
	}
	auto &catalog = db->GetCatalog().Cast<AltertableCatalog>();
	auto &transaction = Transaction::Get(context, catalog).Cast<AltertableTransaction>();
	transaction.ExecuteUpdate(bind_data.sql);
	catalog.ClearCache();

	output.SetCardinality(1);
	output.SetValue(0, 0, Value::BOOLEAN(true));
}

static AltertableCatalog &GetAltertableCatalogByName(ClientContext &context, const string &db_name) {
	auto &db_manager = DatabaseManager::Get(context);
	auto db = db_manager.GetDatabase(context, Identifier(db_name));
	if (!db) {
		throw BinderException("Database \"%s\" not found", db_name);
	}
	if (db->GetCatalog().GetCatalogType() != "altertable") {
		throw BinderException("Database \"%s\" is not an Altertable database", db_name);
	}
	return db->GetCatalog().Cast<AltertableCatalog>();
}

static unique_ptr<FunctionData> AltertableExecuteByNameBind(ClientContext &context, TableFunctionBindInput &input,
                                                            vector<LogicalType> &return_types,
                                                            vector<Identifier> &names) {
	auto result = make_uniq<AltertableExecuteBindData>();
	result->db_name = input.inputs[0].GetValue<string>();
	result->sql = input.inputs[1].GetValue<string>();

	auto &altertable_catalog = GetAltertableCatalogByName(context, result->db_name);
	if (altertable_catalog.access_mode == AccessMode::READ_ONLY) {
		throw BinderException("Cannot execute remote DML on read-only Altertable database \"%s\"", result->db_name);
	}

	names.emplace_back("affected_rows");
	return_types.push_back(LogicalType::BIGINT);
	return std::move(result);
}

static void AltertableExecuteByNameFunc(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<AltertableExecuteBindData>();
	auto &gstate = data.global_state->Cast<AltertableExecuteGlobalState>();

	if (gstate.executed) {
		return;
	}
	gstate.executed = true;

	auto &altertable_catalog = GetAltertableCatalogByName(context, bind_data.db_name);
	auto &transaction = AltertableTransaction::Get(context, altertable_catalog);
	gstate.affected_rows = transaction.ExecuteUpdate(bind_data.sql);
	altertable_catalog.ClearCache();

	output.SetCardinality(1);
	output.SetValue(0, 0, Value::BIGINT(gstate.affected_rows));
}

AltertableExecuteFunction::AltertableExecuteFunction()
    : TableFunction("altertable_execute", {LogicalType::VARCHAR, LogicalType::VARCHAR}, AltertableExecuteFunc,
                    AltertableExecuteBind, AltertableExecuteInitGlobal) {
}

AltertableExecuteByNameFunction::AltertableExecuteByNameFunction()
    : TableFunction("altertable_execute_by_name", {LogicalType::VARCHAR, LogicalType::VARCHAR},
                    AltertableExecuteByNameFunc, AltertableExecuteByNameBind, AltertableExecuteInitGlobal) {
}

} // namespace duckdb
