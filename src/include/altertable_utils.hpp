//===----------------------------------------------------------------------===//
//                         DuckDB
//
// altertable_utils.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "arrow/array.h"
#include "arrow/flight/sql/client.h"
#include "arrow/type.h"

namespace duckdb {
class TableFilter;

struct AltertableTypeData {
	string type_name;
	int32_t numeric_precision = 0;
	int32_t numeric_scale = 0;
};

struct AltertableConnectionConfig {
	string host = "flight.altertable.ai";
	int32_t port = 443;
	string user;
	string password;
	string catalog;
	string compute_size;
	bool ssl = true;

	bool has_host = false;
	bool has_port = false;
	bool has_user = false;
	bool has_password = false;
	bool has_catalog = false;
	bool has_compute_size = false;
	bool has_ssl = false;

	static AltertableConnectionConfig Parse(const string &dsn);

	string ToDSN(bool redact_password = false) const;
};

class AltertableUtils {
public:
	static LogicalType ToAltertableType(const LogicalType &input);
	static LogicalType TypeToLogicalType(const AltertableTypeData &input);
	static string TypeToString(const LogicalType &input);
	static LogicalType RemoveAlias(const LogicalType &type);
	static string QuoteAltertableIdentifier(const string &text);
	static string QuoteDSNValue(const string &value);
	static string QueryFingerprint(const string &query);
	static string QualifiedTableReference(const string &catalog, const string &schema, const string &table);
};

//! Render a DuckDB table filter as Altertable SQL. Returns false when the
//! complete filter tree cannot be represented safely.
bool TryGetAltertablePredicate(TableFilter &filter, const string &column_name, string &predicate);
bool TryGetAltertableComparisonOperator(ExpressionType type, string &comparison_operator);

//! Map Arrow column types to DuckDB logical types (shared by scans and altertable_query).
LogicalType AltertableArrowTypeToLogicalType(const arrow::DataType &arrow_type);

//! Convert `count` values from `array` starting at `offset` into `vector`.
void AltertableConvertArrowArray(Vector &vector, const arrow::Array &array, idx_t offset, idx_t count);

} // namespace duckdb
