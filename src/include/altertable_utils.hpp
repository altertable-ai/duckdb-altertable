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
#include "altertable_version.hpp"

namespace duckdb {
class AltertableSchemaEntry;
class AltertableTransaction;
class AltertableTableEntry;

struct AltertableTypeData {
	int64_t type_modifier = 0;
	string type_name;
};

enum class AltertableTypeAnnotation {
	STANDARD,
	CAST_TO_VARCHAR,
	NUMERIC_AS_DOUBLE,
};

struct AltertableType {
	idx_t oid = 0;
	AltertableTypeAnnotation info = AltertableTypeAnnotation::STANDARD;
	vector<AltertableType> children;
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
	static string Redact(const string &dsn);

	void Merge(const AltertableConnectionConfig &other);
	string ToDSN(bool redact_password = false) const;
};

class AltertableUtils {
public:
	static LogicalType ToAltertableType(const LogicalType &input);
	static LogicalType TypeToLogicalType(optional_ptr<AltertableTransaction> transaction,
	                                     optional_ptr<AltertableSchemaEntry> schema, const AltertableTypeData &input,
	                                     AltertableType &altertable_type);
	static string TypeToString(const LogicalType &input);
	static LogicalType RemoveAlias(const LogicalType &type);
	static AltertableType CreateEmptyAltertableType(const LogicalType &type);
	static string QuoteAltertableIdentifier(const string &text);
	static string QuoteAltertableLiteral(const string &text);

	static AltertableVersion ExtractAltertableVersion(const string &version);
};

//! Map Arrow column types to DuckDB logical types (shared by scans and altertable_query).
LogicalType AltertableArrowTypeToLogicalType(const arrow::DataType &arrow_type);

} // namespace duckdb
