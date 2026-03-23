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
	idx_t array_dimensions = 0;
};

enum class AltertableTypeAnnotation {
	STANDARD,
	CAST_TO_VARCHAR,
	NUMERIC_AS_DOUBLE,
	CTID,
	JSONB,
	FIXED_LENGTH_CHAR,
	GEOM_POINT,
	GEOM_LINE,
	GEOM_LINE_SEGMENT,
	GEOM_BOX,
	GEOM_PATH,
	GEOM_POLYGON,
	GEOM_CIRCLE
};

struct AltertableType {
	idx_t oid = 0;
	AltertableTypeAnnotation info = AltertableTypeAnnotation::STANDARD;
	vector<AltertableType> children;
};

enum class AltertableCopyFormat { AUTO = 0, BINARY = 1, TEXT = 2 };

struct AltertableCopyState {
	AltertableCopyFormat format = AltertableCopyFormat::AUTO;
	bool has_null_byte_replacement = false;
	string null_byte_replacement;

	void Initialize(ClientContext &context);
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

	static AltertableVersion ExtractAltertableVersion(const string &version);
};

//! Map Arrow column types to DuckDB logical types (shared by scans and altertable_query).
LogicalType AltertableArrowTypeToLogicalType(const arrow::DataType &arrow_type);

} // namespace duckdb
