//===----------------------------------------------------------------------===//
//                         DuckDB
//
// altertable_result.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "arrow/table.h"
#include "arrow/array.h"

namespace duckdb {

// Wrapper class for Altertable results from Arrow Flight SQL
// Used for metadata/schema queries and catalog discovery
class AltertableResult {
public:
	AltertableResult() : row_count(0) {
	}

	explicit AltertableResult(std::shared_ptr<arrow::Table> table_p) : table(std::move(table_p)) {
		row_count = table ? table->num_rows() : 0;
	}

	~AltertableResult() {
	}

public:
	string GetString(idx_t row, idx_t col) {
		if (!table || col >= (idx_t)table->num_columns()) {
			return "";
		}
		auto column = table->column(col);
		if (!column || row >= (idx_t)column->length()) {
			return "";
		}
		// Handle chunked arrays
		int64_t chunk_idx = 0;
		int64_t offset = static_cast<int64_t>(row);
		while (chunk_idx < column->num_chunks() && offset >= column->chunk(chunk_idx)->length()) {
			offset -= column->chunk(chunk_idx)->length();
			chunk_idx++;
		}
		if (chunk_idx >= column->num_chunks()) {
			return "";
		}
		auto chunk = column->chunk(chunk_idx);
		if (chunk->IsNull(offset)) {
			return "";
		}
		auto str_array = std::static_pointer_cast<arrow::StringArray>(chunk);
		return str_array->GetString(offset);
	}

	string_t GetStringRef(idx_t row, idx_t col) {
		// For now, just return a string_t from the string
		// Note: This is not truly a reference as the string is copied
		return string_t(GetString(row, col));
	}

	int32_t GetInt32(idx_t row, idx_t col) {
		if (!table || col >= (idx_t)table->num_columns()) {
			return 0;
		}
		auto column = table->column(col);
		if (!column || row >= (idx_t)column->length()) {
			return 0;
		}
		int64_t chunk_idx = 0;
		int64_t offset = static_cast<int64_t>(row);
		while (chunk_idx < column->num_chunks() && offset >= column->chunk(chunk_idx)->length()) {
			offset -= column->chunk(chunk_idx)->length();
			chunk_idx++;
		}
		if (chunk_idx >= column->num_chunks()) {
			return 0;
		}
		auto chunk = column->chunk(chunk_idx);
		if (chunk->IsNull(offset)) {
			return 0;
		}
		auto int_array = std::static_pointer_cast<arrow::Int32Array>(chunk);
		return int_array->Value(offset);
	}

	int64_t GetInt64(idx_t row, idx_t col) {
		if (!table || col >= (idx_t)table->num_columns()) {
			return 0;
		}
		auto column = table->column(col);
		if (!column || row >= (idx_t)column->length()) {
			return 0;
		}
		int64_t chunk_idx = 0;
		int64_t offset = static_cast<int64_t>(row);
		while (chunk_idx < column->num_chunks() && offset >= column->chunk(chunk_idx)->length()) {
			offset -= column->chunk(chunk_idx)->length();
			chunk_idx++;
		}
		if (chunk_idx >= column->num_chunks()) {
			return 0;
		}
		auto chunk = column->chunk(chunk_idx);
		if (chunk->IsNull(offset)) {
			return 0;
		}
		// Handle different integer types
		switch (chunk->type()->id()) {
		case arrow::Type::INT64: {
			auto int_array = std::static_pointer_cast<arrow::Int64Array>(chunk);
			return int_array->Value(offset);
		}
		case arrow::Type::INT32: {
			auto int_array = std::static_pointer_cast<arrow::Int32Array>(chunk);
			return int_array->Value(offset);
		}
		case arrow::Type::INT16: {
			auto int_array = std::static_pointer_cast<arrow::Int16Array>(chunk);
			return int_array->Value(offset);
		}
		default:
			return 0;
		}
	}

	bool GetBool(idx_t row, idx_t col) {
		if (!table || col >= (idx_t)table->num_columns()) {
			return false;
		}
		auto column = table->column(col);
		if (!column || row >= (idx_t)column->length()) {
			return false;
		}
		int64_t chunk_idx = 0;
		int64_t offset = static_cast<int64_t>(row);
		while (chunk_idx < column->num_chunks() && offset >= column->chunk(chunk_idx)->length()) {
			offset -= column->chunk(chunk_idx)->length();
			chunk_idx++;
		}
		if (chunk_idx >= column->num_chunks()) {
			return false;
		}
		auto chunk = column->chunk(chunk_idx);
		if (chunk->IsNull(offset)) {
			return false;
		}
		auto bool_array = std::static_pointer_cast<arrow::BooleanArray>(chunk);
		return bool_array->Value(offset);
	}

	bool IsNull(idx_t row, idx_t col) {
		if (!table || col >= (idx_t)table->num_columns()) {
			return true;
		}
		auto column = table->column(col);
		if (!column || row >= (idx_t)column->length()) {
			return true;
		}
		int64_t chunk_idx = 0;
		int64_t offset = static_cast<int64_t>(row);
		while (chunk_idx < column->num_chunks() && offset >= column->chunk(chunk_idx)->length()) {
			offset -= column->chunk(chunk_idx)->length();
			chunk_idx++;
		}
		if (chunk_idx >= column->num_chunks()) {
			return true;
		}
		return column->chunk(chunk_idx)->IsNull(offset);
	}

	idx_t Count() {
		return row_count;
	}

	idx_t AffectedRows() {
		return row_count;
	}

private:
	std::shared_ptr<arrow::Table> table;
	idx_t row_count;
};

} // namespace duckdb
