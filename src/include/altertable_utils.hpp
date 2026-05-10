//===----------------------------------------------------------------------===//
//                         DuckDB
//
// altertable_utils.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "arrow/flight/sql/client.h"
#include "arrow/type.h"

namespace duckdb {
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
	bool ssl = true;

	bool has_host = false;
	bool has_port = false;
	bool has_user = false;
	bool has_password = false;
	bool has_catalog = false;
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
};

//! Map Arrow column types to DuckDB logical types (shared by scans and altertable_query).
LogicalType AltertableArrowTypeToLogicalType(const arrow::DataType &arrow_type);

} // namespace duckdb
