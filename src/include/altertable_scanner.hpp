//===----------------------------------------------------------------------===//
//                         DuckDB
//
// altertable_scanner.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "altertable_utils.hpp"
#include "duckdb/function/table_function.hpp"
#include "storage/altertable_connection_pool.hpp"

namespace duckdb {
class AltertableCatalog;
class AltertableTableEntry;
struct AltertableLocalState;
struct AltertableGlobalState;
class AltertableTransaction;

struct AltertableBindData : public FunctionData {
public:
	explicit AltertableBindData(ClientContext &context);

	string catalog_name;
	string schema_name;
	string table_name;
	string sql;
	string limit;
	idx_t pages_approx = 0;

	vector<string> names;
	vector<LogicalType> types;

	string dsn;
	string attach_path;

	idx_t max_threads = 1;

public:
	void SetTablePages(idx_t approx_num_pages);

	void SetCatalog(AltertableCatalog &catalog);
	void SetTable(AltertableTableEntry &table);
	optional_ptr<AltertableCatalog> GetCatalog() const {
		return attached_catalog;
	}
	optional_ptr<AltertableTableEntry> GetTable() const {
		return attached_table;
	}

	unique_ptr<FunctionData> Copy() const override {
		throw NotImplementedException("AltertableBindData Copy");
	}
	bool Equals(const FunctionData &other_p) const override {
		return false;
	}

private:
	optional_ptr<AltertableCatalog> attached_catalog;
	optional_ptr<AltertableTableEntry> attached_table;
};

class AltertableScanFunction : public TableFunction {
public:
	AltertableScanFunction();

	static void PrepareBind(AltertableBindData &bind, idx_t approx_num_pages);
};

//! Recognize scans from ATTACH ... (TYPE ALTERTABLE) and altertable_scan(); `function.name` is not always reliable.
bool IsAltertableScanTableFunction(const TableFunction &function);

class AltertableScanFunctionFilterPushdown : public TableFunction {
public:
	AltertableScanFunctionFilterPushdown();
};

class AltertableQueryFunction : public TableFunction {
public:
	AltertableQueryFunction();
};

class AltertableExecuteFunction : public TableFunction {
public:
	AltertableExecuteFunction();
};

} // namespace duckdb
