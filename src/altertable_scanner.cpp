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

#include "arrow/type.h"
#include "arrow/ipc/dictionary.h"
#include "arrow/util/decimal.h"

namespace duckdb {

struct AltertableGlobalState;

struct AltertableLocalState : public LocalTableFunctionState {
	bool done = false;
	AltertableConnection connection;
	AltertablePoolConnection pool_connection;
	std::unique_ptr<arrow::flight::FlightStreamReader> reader;

	void ScanChunk(ClientContext &context, const AltertableBindData &bind_data, AltertableGlobalState &gstate,
	               DataChunk &output);
};

struct AltertableGlobalState : public GlobalTableFunctionState {
	explicit AltertableGlobalState(idx_t max_threads) : max_threads(max_threads) {
	}

	mutable mutex lock;
	idx_t max_threads;
	bool used_main_thread = false;

	AltertableConnection &GetConnection();
	void SetConnection(AltertableConnection connection);
	void SetConnection(shared_ptr<OwnedAltertableConnection> connection);

	bool TryOpenNewConnection(ClientContext &context, AltertableLocalState &lstate,
	                          const AltertableBindData &bind_data);
	idx_t MaxThreads() const override {
		return max_threads;
	}

private:
	AltertableConnection connection;
};

void AltertableScanFunction::PrepareBind(AltertableVersion version, ClientContext &context,
                                         AltertableBindData &bind_data, idx_t approx_num_pages) {
	bind_data.SetTablePages(approx_num_pages);
	bind_data.version = version;
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
	case arrow::Type::FIXED_SIZE_BINARY:
		return LogicalType::BLOB;
	case arrow::Type::DATE32:
	case arrow::Type::DATE64:
		return LogicalType::DATE;
	case arrow::Type::TIME32:
	case arrow::Type::TIME64:
		return LogicalType::TIME;
	case arrow::Type::TIMESTAMP:
		return LogicalType::TIMESTAMP;
	case arrow::Type::DECIMAL128: {
		auto &dec_type = static_cast<const arrow::Decimal128Type &>(arrow_type);
		return LogicalType::DECIMAL(dec_type.precision(), dec_type.scale());
	}
	case arrow::Type::DECIMAL256:
		// DuckDB doesn't support 256-bit decimals, convert to VARCHAR
		return LogicalType::VARCHAR;
	default:
		return LogicalType::VARCHAR; // Fallback
	}
}

static unique_ptr<FunctionData> AltertableBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
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
		table_ref =
		    "\"" + bind_data->catalog_name + "\".\"" + bind_data->schema_name + "\".\"" + bind_data->table_name + "\"";
	} else {
		table_ref = "\"" + bind_data->schema_name + "\".\"" + bind_data->table_name + "\"";
	}
	string query = "SELECT * FROM " + table_ref + " LIMIT 0";
	auto info = con.Execute(query);

	// Get Schema from FlightInfo
	std::shared_ptr<arrow::Schema> schema;
	arrow::ipc::DictionaryMemo memo;
	auto schema_result = info->GetSchema(&memo);
	if (!schema_result.ok()) {
		throw IOException("Failed to get schema from FlightInfo: " + schema_result.status().ToString());
	}
	schema = schema_result.ValueOrDie();

	// Extract column names and types from Arrow schema
	for (int i = 0; i < schema->num_fields(); i++) {
		auto field = schema->field(i);
		names.push_back(field->name());
		return_types.push_back(GetArrowLogicalType(*field->type()));
	}

	bind_data->names = names;
	bind_data->types = return_types;

	return std::move(bind_data);
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
	return std::move(result);
}

bool AltertableGlobalState::TryOpenNewConnection(ClientContext &context, AltertableLocalState &lstate,
                                                 const AltertableBindData &bind_data) {
	auto attached_catalog = bind_data.GetCatalog();
	{
		lock_guard<mutex> parallel_lock(lock);
		if (!used_main_thread) {
			if (bind_data.can_use_main_thread) {
				// Reuse main connection
				// Note: FlightSqlClient might need cloning if not thread-safe or for separate streams
				// For now, let's create a new connection to be safe as FlightSqlClient holds state
				lstate.connection = AltertableConnection::Open(bind_data.dsn);
			} else {
				lstate.connection = AltertableConnection::Open(bind_data.dsn);
			}
			used_main_thread = true;
			return true;
		}
	}

	if (attached_catalog) {
		// Pool not fully adapted for Flight yet
		lstate.connection = AltertableConnection::Open(bind_data.dsn);
	} else {
		lstate.connection = AltertableConnection::Open(bind_data.dsn);
	}
	return true;
}

string GetPredicateFromFilter(TableFilter &filter, const string &col_name) {
	switch (filter.filter_type) {
	case TableFilterType::CONSTANT_COMPARISON: {
		auto &constant_filter = filter.Cast<ConstantFilter>();
		string op;
		switch (constant_filter.comparison_type) {
		case ExpressionType::COMPARE_EQUAL:
			op = "=";
			break;
		case ExpressionType::COMPARE_GREATERTHAN:
			op = ">";
			break;
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			op = ">=";
			break;
		case ExpressionType::COMPARE_LESSTHAN:
			op = "<";
			break;
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
			op = "<=";
			break;
		case ExpressionType::COMPARE_NOTEQUAL:
			op = "<>";
			break;
		default:
			return "";
		}
		return "\"" + col_name + "\" " + op + " " + constant_filter.constant.ToSQLString();
	}
	case TableFilterType::CONJUNCTION_AND: {
		auto &and_filter = filter.Cast<ConjunctionAndFilter>();
		string result;
		for (auto &child_filter : and_filter.child_filters) {
			auto child_predicate = GetPredicateFromFilter(*child_filter, col_name);
			if (child_predicate.empty()) {
				continue;
			}
			if (!result.empty()) {
				result += " AND ";
			}
			result += "(" + child_predicate + ")";
		}
		return result;
	}
	case TableFilterType::CONJUNCTION_OR: {
		auto &or_filter = filter.Cast<ConjunctionOrFilter>();
		string result;
		for (auto &child_filter : or_filter.child_filters) {
			auto child_predicate = GetPredicateFromFilter(*child_filter, col_name);
			if (child_predicate.empty()) {
				return "";
			}
			if (!result.empty()) {
				result += " OR ";
			}
			result += "(" + child_predicate + ")";
		}
		return result;
	}
	case TableFilterType::IS_NULL:
		return "\"" + col_name + "\" IS NULL";
	case TableFilterType::IS_NOT_NULL:
		return "\"" + col_name + "\" IS NOT NULL";
	default:
		return "";
	}
}

static unique_ptr<LocalTableFunctionState> GetLocalState(ClientContext &context, TableFunctionInitInput &input,
                                                         AltertableGlobalState &gstate) {
	auto &bind_data = (AltertableBindData &)*input.bind_data;
	auto local_state = make_uniq<AltertableLocalState>();

	if (!gstate.TryOpenNewConnection(context, *local_state, bind_data)) {
		return nullptr;
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

		// Use column ids to select specific columns (projection pushdown)
		if (input.column_ids.empty()) {
			query += "*";
		} else {
			for (auto &col_idx : input.column_ids) {
				if (col_idx == COLUMN_IDENTIFIER_ROW_ID)
					continue;
				if (!first) {
					query += ", ";
				}
				if (col_idx < bind_data.names.size()) {
					query += "\"" + bind_data.names[col_idx] + "\"";
				}
				first = false;
			}
		}

		if (first) { // No columns selected or only rowid
			query = "SELECT *";
		}

		// Use three-part identifier if catalog is set
		if (!bind_data.catalog_name.empty()) {
			query += " FROM \"" + bind_data.catalog_name + "\".\"" + bind_data.schema_name + "\".\"" +
			         bind_data.table_name + "\"";
		} else {
			query += " FROM \"" + bind_data.schema_name + "\".\"" + bind_data.table_name + "\"";
		}

		if (input.filters) {
			string filter_string;
			for (auto &entry : input.filters->filters) {
				idx_t col_idx = entry.first;
				auto &filter = *entry.second;
				if (col_idx < bind_data.names.size()) {
					string col_name = bind_data.names[col_idx];
					string predicate = GetPredicateFromFilter(filter, col_name);
					if (!predicate.empty()) {
						if (!filter_string.empty()) {
							filter_string += " AND ";
						}
						filter_string += predicate;
					}
				}
			}
			if (!filter_string.empty()) {
				query += " WHERE " + filter_string;
			}
		}

		if (!bind_data.limit.empty()) {
			query += bind_data.limit;
		}
	}

	// Execute query to get stream reader from Altertable
	local_state->reader = local_state->connection.QueryStream(query);

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

	auto chunk_result = reader->Next();

	if (!chunk_result.ok()) {
		throw IOException("Failed to read next chunk: " + chunk_result.status().ToString());
	}

	auto chunk = chunk_result.ValueOrDie();

	if (!chunk.data) {
		done = true;
		return;
	}

	// Convert Arrow RecordBatch to DuckDB DataChunk
	// We use DuckDB's internal Arrow converter
	// Need to cast to arrow::Table for the converter or use lower level

	// Simple conversion for now:
	// We can use the DuckDB Arrow extension helper or manual conversion
	// Since we are IN DuckDB, we can use ArrowToDuckDB

	// But ArrowToDuckDB usually expects a table or stream structure

	// Let's rely on standard Arrow conversion available in DuckDB
	// We need to implement manual conversion if headers are not exposed

	// For now, assuming we have ArrowToDuckDB available or implement basic loop

	// Note: This is a placeholder for actual Arrow -> DuckDB conversion
	// which involves iterating columns and appending to DataChunk

	auto batch = chunk.data;
	idx_t row_count = batch->num_rows();

	if (row_count == 0)
		return;

	// Standard DuckDB vector size
	if (row_count > STANDARD_VECTOR_SIZE) {
		// Truncate to standard vector size for now
		// TODO: Handle larger batches by buffering
		row_count = STANDARD_VECTOR_SIZE;
	}

	// Using DuckDB's Arrow converter
	// We need to pass the arrow array pointers
	// This part requires access to duckdb's arrow_wrapper or similar

	// Simplest approach: use ArrowTableFunction's logic if possible
	// or manually copy data

	// Manual copy for common types
	output.SetCardinality(row_count);

	for (idx_t col_idx = 0; col_idx < output.ColumnCount(); col_idx++) {
		auto &vector = output.data[col_idx];
		auto arrow_col = batch->column(col_idx);

		// Comprehensive type conversion from Arrow to DuckDB
		switch (arrow_col->type()->id()) {
		case arrow::Type::BOOL: {
			auto bool_array = std::static_pointer_cast<arrow::BooleanArray>(arrow_col);
			auto data = FlatVector::GetData<bool>(vector);
			for (idx_t i = 0; i < row_count; i++) {
				if (bool_array->IsValid(i)) {
					data[i] = bool_array->Value(i);
				} else {
					FlatVector::SetNull(vector, i, true);
				}
			}
			break;
		}
		case arrow::Type::INT8: {
			auto int_array = std::static_pointer_cast<arrow::Int8Array>(arrow_col);
			auto data = FlatVector::GetData<int8_t>(vector);
			for (idx_t i = 0; i < row_count; i++) {
				if (int_array->IsValid(i)) {
					data[i] = int_array->Value(i);
				} else {
					FlatVector::SetNull(vector, i, true);
				}
			}
			break;
		}
		case arrow::Type::INT16: {
			auto int_array = std::static_pointer_cast<arrow::Int16Array>(arrow_col);
			auto data = FlatVector::GetData<int16_t>(vector);
			for (idx_t i = 0; i < row_count; i++) {
				if (int_array->IsValid(i)) {
					data[i] = int_array->Value(i);
				} else {
					FlatVector::SetNull(vector, i, true);
				}
			}
			break;
		}
		case arrow::Type::INT32: {
			auto int_array = std::static_pointer_cast<arrow::Int32Array>(arrow_col);
			auto data = FlatVector::GetData<int32_t>(vector);
			for (idx_t i = 0; i < row_count; i++) {
				if (int_array->IsValid(i)) {
					data[i] = int_array->Value(i);
				} else {
					FlatVector::SetNull(vector, i, true);
				}
			}
			break;
		}
		case arrow::Type::INT64: {
			auto int_array = std::static_pointer_cast<arrow::Int64Array>(arrow_col);
			auto data = FlatVector::GetData<int64_t>(vector);
			for (idx_t i = 0; i < row_count; i++) {
				if (int_array->IsValid(i)) {
					data[i] = int_array->Value(i);
				} else {
					FlatVector::SetNull(vector, i, true);
				}
			}
			break;
		}
		case arrow::Type::UINT8: {
			auto int_array = std::static_pointer_cast<arrow::UInt8Array>(arrow_col);
			auto data = FlatVector::GetData<uint8_t>(vector);
			for (idx_t i = 0; i < row_count; i++) {
				if (int_array->IsValid(i)) {
					data[i] = int_array->Value(i);
				} else {
					FlatVector::SetNull(vector, i, true);
				}
			}
			break;
		}
		case arrow::Type::UINT16: {
			auto int_array = std::static_pointer_cast<arrow::UInt16Array>(arrow_col);
			auto data = FlatVector::GetData<uint16_t>(vector);
			for (idx_t i = 0; i < row_count; i++) {
				if (int_array->IsValid(i)) {
					data[i] = int_array->Value(i);
				} else {
					FlatVector::SetNull(vector, i, true);
				}
			}
			break;
		}
		case arrow::Type::UINT32: {
			auto int_array = std::static_pointer_cast<arrow::UInt32Array>(arrow_col);
			auto data = FlatVector::GetData<uint32_t>(vector);
			for (idx_t i = 0; i < row_count; i++) {
				if (int_array->IsValid(i)) {
					data[i] = int_array->Value(i);
				} else {
					FlatVector::SetNull(vector, i, true);
				}
			}
			break;
		}
		case arrow::Type::UINT64: {
			auto int_array = std::static_pointer_cast<arrow::UInt64Array>(arrow_col);
			auto data = FlatVector::GetData<uint64_t>(vector);
			for (idx_t i = 0; i < row_count; i++) {
				if (int_array->IsValid(i)) {
					data[i] = int_array->Value(i);
				} else {
					FlatVector::SetNull(vector, i, true);
				}
			}
			break;
		}
		case arrow::Type::FLOAT: {
			auto float_array = std::static_pointer_cast<arrow::FloatArray>(arrow_col);
			auto data = FlatVector::GetData<float>(vector);
			for (idx_t i = 0; i < row_count; i++) {
				if (float_array->IsValid(i)) {
					data[i] = float_array->Value(i);
				} else {
					FlatVector::SetNull(vector, i, true);
				}
			}
			break;
		}
		case arrow::Type::DOUBLE: {
			auto double_array = std::static_pointer_cast<arrow::DoubleArray>(arrow_col);
			auto data = FlatVector::GetData<double>(vector);
			for (idx_t i = 0; i < row_count; i++) {
				if (double_array->IsValid(i)) {
					data[i] = double_array->Value(i);
				} else {
					FlatVector::SetNull(vector, i, true);
				}
			}
			break;
		}
		case arrow::Type::STRING: {
			auto str_array = std::static_pointer_cast<arrow::StringArray>(arrow_col);
			// Check if we're supposed to write to a UUID column
			// This can happen when information_schema says UUID but Arrow returns STRING
			if (vector.GetType() == LogicalType::UUID) {
				// Parse UUID string and write as INT128
				auto data = FlatVector::GetData<hugeint_t>(vector);
				for (idx_t i = 0; i < row_count; i++) {
					if (str_array->IsValid(i)) {
						string uuid_str = str_array->GetString(i);
						// Parse UUID string: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
						// Remove hyphens and parse as hex
						string hex_str;
						for (char c : uuid_str) {
							if (c != '-') {
								hex_str += c;
							}
						}
						// Convert hex string to INT128
						uint64_t upper = 0;
						uint64_t lower = 0;
						for (size_t j = 0; j < 16 && j < hex_str.size(); j++) {
							int nibble = (hex_str[j] >= '0' && hex_str[j] <= '9') ? (hex_str[j] - '0')
							                                                      : (tolower(hex_str[j]) - 'a' + 10);
							upper = (upper << 4) | nibble;
						}
						for (size_t j = 16; j < 32 && j < hex_str.size(); j++) {
							int nibble = (hex_str[j] >= '0' && hex_str[j] <= '9') ? (hex_str[j] - '0')
							                                                      : (tolower(hex_str[j]) - 'a' + 10);
							lower = (lower << 4) | nibble;
						}
						data[i] = hugeint_t(upper, lower);
					} else {
						FlatVector::SetNull(vector, i, true);
					}
				}
			} else {
				// Normal VARCHAR handling
				auto data = FlatVector::GetData<string_t>(vector);
				for (idx_t i = 0; i < row_count; i++) {
					if (str_array->IsValid(i)) {
						string val = str_array->GetString(i);
						data[i] = StringVector::AddString(vector, val);
					} else {
						FlatVector::SetNull(vector, i, true);
					}
				}
			}
			break;
		}
		case arrow::Type::LARGE_STRING: {
			auto str_array = std::static_pointer_cast<arrow::LargeStringArray>(arrow_col);
			auto data = FlatVector::GetData<string_t>(vector);
			for (idx_t i = 0; i < row_count; i++) {
				if (str_array->IsValid(i)) {
					string val = str_array->GetString(i);
					data[i] = StringVector::AddString(vector, val);
				} else {
					FlatVector::SetNull(vector, i, true);
				}
			}
			break;
		}
		case arrow::Type::BINARY: {
			auto bin_array = std::static_pointer_cast<arrow::BinaryArray>(arrow_col);
			auto data = FlatVector::GetData<string_t>(vector);
			for (idx_t i = 0; i < row_count; i++) {
				if (bin_array->IsValid(i)) {
					auto view = bin_array->GetView(i);
					data[i] = StringVector::AddStringOrBlob(vector, view.data(), view.size());
				} else {
					FlatVector::SetNull(vector, i, true);
				}
			}
			break;
		}
		case arrow::Type::LARGE_BINARY: {
			auto bin_array = std::static_pointer_cast<arrow::LargeBinaryArray>(arrow_col);
			auto data = FlatVector::GetData<string_t>(vector);
			for (idx_t i = 0; i < row_count; i++) {
				if (bin_array->IsValid(i)) {
					auto view = bin_array->GetView(i);
					data[i] = StringVector::AddStringOrBlob(vector, view.data(), view.size());
				} else {
					FlatVector::SetNull(vector, i, true);
				}
			}
			break;
		}
		case arrow::Type::DATE32: {
			auto date_array = std::static_pointer_cast<arrow::Date32Array>(arrow_col);
			auto data = FlatVector::GetData<date_t>(vector);
			for (idx_t i = 0; i < row_count; i++) {
				if (date_array->IsValid(i)) {
					// Arrow DATE32 is days since Unix epoch
					// DuckDB date_t is also days since Unix epoch (but different epoch)
					int32_t arrow_days = date_array->Value(i);
					data[i] = date_t(arrow_days);
				} else {
					FlatVector::SetNull(vector, i, true);
				}
			}
			break;
		}
		case arrow::Type::DATE64: {
			auto date_array = std::static_pointer_cast<arrow::Date64Array>(arrow_col);
			auto data = FlatVector::GetData<date_t>(vector);
			for (idx_t i = 0; i < row_count; i++) {
				if (date_array->IsValid(i)) {
					// Arrow DATE64 is milliseconds since Unix epoch
					// Convert to days
					int64_t arrow_ms = date_array->Value(i);
					int32_t days = static_cast<int32_t>(arrow_ms / (1000 * 60 * 60 * 24));
					data[i] = date_t(days);
				} else {
					FlatVector::SetNull(vector, i, true);
				}
			}
			break;
		}
		case arrow::Type::TIMESTAMP: {
			auto ts_array = std::static_pointer_cast<arrow::TimestampArray>(arrow_col);
			auto ts_type = std::static_pointer_cast<arrow::TimestampType>(arrow_col->type());
			auto data = FlatVector::GetData<timestamp_t>(vector);

			// Get the time unit to convert to microseconds (DuckDB's native format)
			auto unit = ts_type->unit();

			for (idx_t i = 0; i < row_count; i++) {
				if (ts_array->IsValid(i)) {
					int64_t value = ts_array->Value(i);
					int64_t micros;

					switch (unit) {
					case arrow::TimeUnit::SECOND:
						micros = value * 1000000;
						break;
					case arrow::TimeUnit::MILLI:
						micros = value * 1000;
						break;
					case arrow::TimeUnit::MICRO:
						micros = value;
						break;
					case arrow::TimeUnit::NANO:
						micros = value / 1000;
						break;
					}

					data[i] = timestamp_t(micros);
				} else {
					FlatVector::SetNull(vector, i, true);
				}
			}
			break;
		}
		case arrow::Type::TIME32: {
			auto time_array = std::static_pointer_cast<arrow::Time32Array>(arrow_col);
			auto time_type = std::static_pointer_cast<arrow::Time32Type>(arrow_col->type());
			auto data = FlatVector::GetData<dtime_t>(vector);

			for (idx_t i = 0; i < row_count; i++) {
				if (time_array->IsValid(i)) {
					int32_t value = time_array->Value(i);
					int64_t micros;

					if (time_type->unit() == arrow::TimeUnit::SECOND) {
						micros = static_cast<int64_t>(value) * 1000000;
					} else { // MILLI
						micros = static_cast<int64_t>(value) * 1000;
					}

					data[i] = dtime_t(micros);
				} else {
					FlatVector::SetNull(vector, i, true);
				}
			}
			break;
		}
		case arrow::Type::TIME64: {
			auto time_array = std::static_pointer_cast<arrow::Time64Array>(arrow_col);
			auto time_type = std::static_pointer_cast<arrow::Time64Type>(arrow_col->type());
			auto data = FlatVector::GetData<dtime_t>(vector);

			for (idx_t i = 0; i < row_count; i++) {
				if (time_array->IsValid(i)) {
					int64_t value = time_array->Value(i);
					int64_t micros;

					if (time_type->unit() == arrow::TimeUnit::MICRO) {
						micros = value;
					} else { // NANO
						micros = value / 1000;
					}

					data[i] = dtime_t(micros);
				} else {
					FlatVector::SetNull(vector, i, true);
				}
			}
			break;
		}
		case arrow::Type::FIXED_SIZE_BINARY: {
			// Handle as BLOB
			auto fixed_array = std::static_pointer_cast<arrow::FixedSizeBinaryArray>(arrow_col);
			auto data = FlatVector::GetData<string_t>(vector);
			for (idx_t i = 0; i < row_count; i++) {
				if (fixed_array->IsValid(i)) {
					auto view = fixed_array->GetView(i);
					data[i] = StringVector::AddStringOrBlob(vector, view.data(), view.size());
				} else {
					FlatVector::SetNull(vector, i, true);
				}
			}
			break;
		}
		case arrow::Type::DECIMAL128: {
			auto dec_array = std::static_pointer_cast<arrow::Decimal128Array>(arrow_col);
			auto dec_type = std::static_pointer_cast<arrow::Decimal128Type>(arrow_col->type());
			auto data = FlatVector::GetData<hugeint_t>(vector);

			for (idx_t i = 0; i < row_count; i++) {
				if (dec_array->IsValid(i)) {
					// GetView returns raw bytes, construct Decimal128 from the pointer
					auto view = dec_array->GetView(i);
					arrow::Decimal128 decimal_value(reinterpret_cast<const uint8_t *>(view.data()));
					// Convert to hugeint_t - Arrow Decimal128 is stored as two 64-bit integers
					auto low = static_cast<uint64_t>(decimal_value.low_bits());
					auto high = static_cast<int64_t>(decimal_value.high_bits());
					data[i] = hugeint_t(high, low);
				} else {
					FlatVector::SetNull(vector, i, true);
				}
			}
			break;
		}
		case arrow::Type::DECIMAL256: {
			// For DECIMAL256, convert to string representation as DuckDB doesn't natively support 256-bit decimals
			auto dec_array = std::static_pointer_cast<arrow::Decimal256Array>(arrow_col);
			auto dec_type = std::static_pointer_cast<arrow::Decimal256Type>(arrow_col->type());
			auto data = FlatVector::GetData<string_t>(vector);

			for (idx_t i = 0; i < row_count; i++) {
				if (dec_array->IsValid(i)) {
					auto view = dec_array->GetView(i);
					arrow::Decimal256 decimal_value(reinterpret_cast<const uint8_t *>(view.data()));
					string str_val = decimal_value.ToString(dec_type->scale());
					data[i] = StringVector::AddString(vector, str_val);
				} else {
					FlatVector::SetNull(vector, i, true);
				}
			}
			break;
		}
		default:
			// Log the unknown type for debugging
			throw NotImplementedException("Arrow type %s (ID: %d) not yet supported for column index %llu",
			                              arrow_col->type()->ToString(), (int)arrow_col->type()->id(), col_idx);
		}
	}
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

AltertableScanFunction::AltertableScanFunction()
    : TableFunction("altertable_scan", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
                    AltertableScan, AltertableBind, AltertableInitGlobalState, AltertableInitLocalState) {
	cardinality = AltertableScanCardinality;
	projection_pushdown = true;
	filter_pushdown = true;
}

AltertableScanFunctionFilterPushdown::AltertableScanFunctionFilterPushdown()
    : TableFunction("altertable_scan_pushdown", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
                    AltertableScan, AltertableBind, AltertableInitGlobalState, AltertableInitLocalState) {
	cardinality = AltertableScanCardinality;
	projection_pushdown = true;
	filter_pushdown = true;
}

} // namespace duckdb
