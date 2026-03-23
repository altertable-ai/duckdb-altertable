//===----------------------------------------------------------------------===//
//                         DuckDB
//
// altertable_scanner.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "altertable_utils.hpp"
#include "storage/altertable_connection_pool.hpp"

namespace duckdb {
class AltertableCatalog;
struct AltertableLocalState;
struct AltertableGlobalState;
class AltertableTransaction;

struct AltertableBindData : public FunctionData {
	static constexpr const idx_t DEFAULT_PAGES_PER_TASK = 1000;

public:
	explicit AltertableBindData(ClientContext &context);

	AltertableVersion version;
	string catalog_name;
	string schema_name;
	string table_name;
	string sql;
	string limit;
	idx_t pages_approx = 0;

	vector<AltertableType> altertable_types;
	vector<string> names;
	vector<LogicalType> types;

	idx_t pages_per_task = DEFAULT_PAGES_PER_TASK;
	string dsn;
	string attach_path;

	bool requires_materialization = true;
	bool can_use_main_thread = true;
	bool read_only = true;
	bool emit_ctid = false;
	bool use_text_protocol = false;
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

	static void PrepareBind(AltertableVersion version, ClientContext &context, AltertableBindData &bind,
	                        idx_t approx_num_pages);
};

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
