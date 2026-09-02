#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/helper.hpp"
#include "altertable_scanner.hpp"
#include "storage/altertable_catalog.hpp"
#include "storage/altertable_transaction.hpp"
#include "storage/altertable_table_set.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/table_filter_set.hpp"

#include "arrow/type.h"
#include "arrow/ipc/dictionary.h"
#include "arrow/util/decimal.h"

namespace duckdb {

struct AltertableGlobalState;

struct AltertableLocalState : public LocalTableFunctionState {
	bool done = false;
	AltertableConnection connection;
	AltertablePoolConnection pool_connection;
	std::unique_ptr<arrow::flight::FlightInfo> flight_info;
	idx_t endpoint_idx = 0;
	std::unique_ptr<arrow::flight::FlightStreamReader> reader;
	std::shared_ptr<arrow::RecordBatch> current_batch;
	idx_t batch_offset = 0;
	DataChunk all_columns;

	void ScanChunk(ClientContext &context, const AltertableBindData &bind_data, AltertableGlobalState &gstate,
	               DataChunk &output);
};

struct AltertableGlobalState : public GlobalTableFunctionState {
	explicit AltertableGlobalState(idx_t max_threads) : max_threads(max_threads) {
	}

	mutable mutex lock;
	idx_t max_threads;
	bool used_main_thread = false;
	vector<idx_t> projection_ids;
	vector<LogicalType> scanned_types;

	AltertableConnection &GetConnection();
	void SetConnection(AltertableConnection connection);
	void SetConnection(shared_ptr<OwnedAltertableConnection> connection);

	bool TryOpenNewConnection(ClientContext &context, AltertableLocalState &lstate,
	                          const AltertableBindData &bind_data);
	bool UseProjectionMapping() const {
		return !projection_ids.empty();
	}
	idx_t MaxThreads() const override {
		return max_threads;
	}

private:
	AltertableConnection connection;
};

void AltertableScanFunction::PrepareBind(AltertableBindData &bind_data, idx_t approx_num_pages) {
	bind_data.SetTablePages(approx_num_pages);
}

AltertableBindData::AltertableBindData(ClientContext &context) {
}

void AltertableBindData::SetTablePages(idx_t approx_num_pages) {
	this->pages_approx = approx_num_pages;
	max_threads = 1; // Flight SQL stream usually single threaded per stream
}

AltertableConnection &AltertableGlobalState::GetConnection() {
	return connection;
}

void AltertableGlobalState::SetConnection(AltertableConnection connection) {
	this->connection = std::move(connection);
}

void AltertableGlobalState::SetConnection(shared_ptr<OwnedAltertableConnection> connection) {
	this->connection = AltertableConnection(std::move(connection));
}

void AltertableBindData::SetCatalog(AltertableCatalog &catalog) {
	this->attached_catalog = &catalog;
}

void AltertableBindData::SetTable(AltertableTableEntry &table) {
	this->attached_table = &table;
}

static unique_ptr<FunctionData> AltertableBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto bind_data = make_uniq<AltertableBindData>(context);

	bind_data->dsn = input.inputs[0].GetValue<string>();
	bind_data->schema_name = input.inputs[1].GetValue<string>();
	bind_data->table_name = input.inputs[2].GetValue<string>();
	bind_data->attach_path = bind_data->dsn;

	auto con = AltertableConnection::Open(bind_data->dsn);
	bind_data->catalog_name = con.GetCatalog();

	// Query schema for the table - use three-part identifier if catalog is set
	string table_ref;
	if (!bind_data->catalog_name.empty()) {
		table_ref = AltertableUtils::QuoteAltertableIdentifier(bind_data->catalog_name) + "." +
		            AltertableUtils::QuoteAltertableIdentifier(bind_data->schema_name) + "." +
		            AltertableUtils::QuoteAltertableIdentifier(bind_data->table_name);
	} else {
		table_ref = AltertableUtils::QuoteAltertableIdentifier(bind_data->schema_name) + "." +
		            AltertableUtils::QuoteAltertableIdentifier(bind_data->table_name);
	}
	string query = "SELECT * FROM " + table_ref + " LIMIT 0";
	auto schema = con.GetExecuteSchema(query);

	// Extract column names and types from Arrow schema
	for (int i = 0; i < schema->num_fields(); i++) {
		auto field = schema->field(i);
		names.emplace_back(field->name());
		return_types.push_back(AltertableArrowTypeToLogicalType(*field->type()));
	}

	bind_data->names = IdentifiersToStrings(names);
	bind_data->types = return_types;

	return std::move(bind_data);
}

static string AltertableTableReference(const AltertableBindData &bind_data) {
	return AltertableUtils::QualifiedTableReference(bind_data.catalog_name, bind_data.schema_name,
	                                                bind_data.table_name);
}

static unique_ptr<LocalTableFunctionState> GetLocalState(ClientContext &context, TableFunctionInitInput &input,
                                                         AltertableGlobalState &gstate);

static unique_ptr<GlobalTableFunctionState> AltertableInitGlobalState(ClientContext &context,
                                                                      TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<AltertableBindData>();
	auto result = make_uniq<AltertableGlobalState>(bind_data.max_threads);
	auto attached_catalog = bind_data.GetCatalog();
	if (attached_catalog) {
		auto &transaction = Transaction::Get(context, *attached_catalog).Cast<AltertableTransaction>();
		auto &con = transaction.GetConnection();
		// Get the shared connection pointer and create a new AltertableConnection from it
		result->SetConnection(con.GetSharedConnection());
	} else {
		auto con = AltertableConnection::Open(bind_data.dsn);
		result->SetConnection(std::move(con));
	}
	if (!input.projection_ids.empty()) {
		result->projection_ids = input.projection_ids;
		for (const auto &col_idx : input.column_ids) {
			if (col_idx == COLUMN_IDENTIFIER_ROW_ID) {
				result->scanned_types.emplace_back(LogicalType(LogicalTypeId::BIGINT));
			} else if (col_idx < bind_data.types.size()) {
				result->scanned_types.push_back(bind_data.types[col_idx]);
			}
		}
	}
	return std::move(result);
}

bool AltertableGlobalState::TryOpenNewConnection(ClientContext &context, AltertableLocalState &lstate,
                                                 const AltertableBindData &bind_data) {
	auto attached_catalog = bind_data.GetCatalog();
	{
		lock_guard<mutex> parallel_lock(lock);
		if (!used_main_thread) {
			if (attached_catalog) {
				lstate.connection = AltertableConnection(GetConnection().GetSharedConnection());
			} else {
				lstate.connection = AltertableConnection::Open(bind_data.dsn);
			}
			used_main_thread = true;
			return true;
		}
	}

	lstate.connection = AltertableConnection::Open(bind_data.dsn);
	return true;
}

static unique_ptr<LocalTableFunctionState> GetLocalState(ClientContext &context, TableFunctionInitInput &input,
                                                         AltertableGlobalState &gstate) {
	auto &bind_data = (AltertableBindData &)*input.bind_data;
	auto local_state = make_uniq<AltertableLocalState>();

	if (!gstate.TryOpenNewConnection(context, *local_state, bind_data)) {
		return nullptr;
	}

	if (gstate.UseProjectionMapping()) {
		local_state->all_columns.Initialize(context, gstate.scanned_types);
	}

	// Construct Query - forward to Altertable for execution
	string query;
	if (!bind_data.sql.empty()) {
		// Direct SQL query (from altertable_query function)
		query = bind_data.sql;
	} else {
		// Table scan - build SELECT query
		query = "SELECT ";
		bool first = true;

		// Use column ids to select specific columns (projection pushdown).
		// Filter-only columns are included in column_ids for WHERE pushdown but stripped
		// from output via projection_ids in ScanChunk (see filter_prune).
		if (input.column_ids.empty()) {
			query += "*";
		} else {
			for (auto &col_idx : input.column_ids) {
				if (col_idx == COLUMN_IDENTIFIER_ROW_ID) {
					continue;
				}
				if (!first) {
					query += ", ";
				}
				if (col_idx < bind_data.names.size()) {
					query += AltertableUtils::QuoteAltertableIdentifier(bind_data.names[col_idx]);
				}
				first = false;
			}
		}

		if (first) { // No columns selected or only rowid
			query = "SELECT *";
		}

		query += " FROM " + AltertableTableReference(bind_data);

		if (input.filters) {
			string filter_string;
			for (auto &entry : *input.filters) {
				idx_t projected_col_idx = entry.GetIndex().GetIndexUnsafe();
				auto &filter = entry.Filter();

				// Map from projected column index to original table column index
				// When filters are pushed down with projection, the filter column indices
				// refer to the projected columns (column_ids), not the original table
				idx_t table_col_idx;
				if (!input.column_ids.empty()) {
					// Projection pushdown is active - map through column_ids
					if (projected_col_idx >= input.column_ids.size()) {
						throw InternalException("Filter column index %llu is out of range for column_ids (size: %llu)",
						                        projected_col_idx, input.column_ids.size());
					}
					table_col_idx = input.column_ids[projected_col_idx];
				} else {
					// No projection pushdown - filter indices refer directly to table columns
					table_col_idx = projected_col_idx;
				}

				if (table_col_idx >= bind_data.names.size()) {
					throw InternalException("Table column index %llu is out of range for table columns (size: %llu)",
					                        table_col_idx, bind_data.names.size());
				}

				string predicate;
				if (!TryGetAltertablePredicate(filter, bind_data.names[table_col_idx], predicate)) {
					throw NotImplementedException("Unsupported table filter for Altertable pushdown");
				}
				if (predicate.empty()) {
					continue;
				}
				if (!filter_string.empty()) {
					filter_string += " AND ";
				}
				filter_string += predicate;
			}
			if (!filter_string.empty()) {
				query += " WHERE " + filter_string;
			}
		}

		if (!bind_data.limit.empty()) {
			query += bind_data.limit;
		}
	}

	// Execute query to get all Flight endpoints. FlightInfo may describe
	// multiple streams, and Flight requires clients to consume every endpoint.
	local_state->flight_info = local_state->connection.Execute(query);
	if (local_state->flight_info->endpoints().empty()) {
		throw IOException("No endpoints returned for query");
	}
	local_state->reader =
	    local_state->connection.QueryEndpointStream(local_state->flight_info->endpoints()[local_state->endpoint_idx]);

	return std::move(local_state);
}

static unique_ptr<LocalTableFunctionState> AltertableInitLocalState(ExecutionContext &context,
                                                                    TableFunctionInitInput &input,
                                                                    GlobalTableFunctionState *global_state) {
	auto &gstate = global_state->Cast<AltertableGlobalState>();
	return GetLocalState(context.client, input, gstate);
}

void AltertableLocalState::ScanChunk(ClientContext &context, const AltertableBindData &bind_data,
                                     AltertableGlobalState &gstate, DataChunk &output) {
	if (done)
		return;

	while (!current_batch || batch_offset >= (idx_t)current_batch->num_rows()) {
		auto chunk_result = reader->Next();

		if (!chunk_result.ok()) {
			throw IOException("Failed to read next chunk: " + chunk_result.status().ToString());
		}

		auto chunk = chunk_result.ValueOrDie();

		if (!chunk.data) {
			endpoint_idx++;
			current_batch.reset();
			batch_offset = 0;
			if (!flight_info || endpoint_idx >= flight_info->endpoints().size()) {
				done = true;
				return;
			}
			reader = connection.QueryEndpointStream(flight_info->endpoints()[endpoint_idx]);
			continue;
		}
		current_batch = chunk.data;
		batch_offset = 0;
	}

	auto batch = current_batch;
	idx_t available_rows = batch->num_rows() - batch_offset;
	idx_t row_count = MinValue<idx_t>(available_rows, STANDARD_VECTOR_SIZE);

	if (row_count == 0)
		return;

	// Manual copy for common types
	auto &scan_chunk = gstate.UseProjectionMapping() ? all_columns : output;
	if (gstate.UseProjectionMapping()) {
		all_columns.Reset();
	}
	auto expected_columns = gstate.UseProjectionMapping() ? gstate.scanned_types.size() : bind_data.types.size();
	if (batch->num_columns() != expected_columns) {
		throw IOException("Altertable returned %llu columns, but DuckDB expected %llu", batch->num_columns(),
		                  expected_columns);
	}
	scan_chunk.SetCardinality(row_count);

	for (idx_t col_idx = 0; col_idx < scan_chunk.ColumnCount(); col_idx++) {
		auto &vector = scan_chunk.data[col_idx];
		auto arrow_col = batch->column(col_idx);
		AltertableConvertArrowArray(vector, *arrow_col, batch_offset, row_count);
	}
	if (gstate.UseProjectionMapping()) {
		output.ReferenceColumns(all_columns, gstate.projection_ids);
	}
	batch_offset += row_count;
}

static void AltertableScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<AltertableBindData>();
	auto &gstate = data.global_state->Cast<AltertableGlobalState>();
	auto &local_state = data.local_state->Cast<AltertableLocalState>();

	local_state.ScanChunk(context, bind_data, gstate, output);
}

static unique_ptr<NodeStatistics> AltertableScanCardinality(ClientContext &context, const FunctionData *bind_data_p) {
	auto &bind_data = bind_data_p->Cast<AltertableBindData>();
	return make_uniq<NodeStatistics>(bind_data.pages_approx * 100); // Rough estimate
}

static BindInfo AltertableScanGetBindInfo(const optional_ptr<FunctionData> bind_data_p) {
	auto &bind_data = bind_data_p->Cast<AltertableBindData>();
	auto table = bind_data.GetTable();
	if (!table) {
		return BindInfo(ScanType::EXTERNAL);
	}
	return BindInfo(*table);
}

static InsertionOrderPreservingMap<string> AltertableScanToString(TableFunctionToStringInput &input) {
	auto &bind_data = input.bind_data->Cast<AltertableBindData>();
	InsertionOrderPreservingMap<string> result;
	result["Function"] = "ALTERTABLE_SCAN";
	if (!bind_data.sql.empty()) {
		result["Query"] = AltertableUtils::QueryFingerprint(bind_data.sql);
	}
	return result;
}

AltertableScanFunction::AltertableScanFunction()
    : TableFunction("altertable_scan", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
                    AltertableScan, AltertableBind, AltertableInitGlobalState, AltertableInitLocalState) {
	cardinality = AltertableScanCardinality;
	get_bind_info = AltertableScanGetBindInfo;
	to_string = AltertableScanToString;
	projection_pushdown = true;
	filter_pushdown = true;
	filter_prune = true;
}

AltertableScanFunctionFilterPushdown::AltertableScanFunctionFilterPushdown()
    : TableFunction("altertable_scan_pushdown", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
                    AltertableScan, AltertableBind, AltertableInitGlobalState, AltertableInitLocalState) {
	cardinality = AltertableScanCardinality;
	get_bind_info = AltertableScanGetBindInfo;
	to_string = AltertableScanToString;
	projection_pushdown = true;
	filter_pushdown = true;
	filter_prune = true;
}

bool IsAltertableScanTableFunction(const TableFunction &function) {
	static AltertableScanFunction scan;
	static AltertableScanFunctionFilterPushdown scan_pushdown;
	if (function.function == scan.function && function.bind == scan.bind) {
		return true;
	}
	if (function.function == scan_pushdown.function && function.bind == scan_pushdown.bind) {
		return true;
	}
	return function.name == "altertable_scan" || function.name == "altertable_scan_pushdown" ||
	       function.name == "altertable_query" || function.name == "altertable_query_by_name";
}

} // namespace duckdb
