#include "altertable_utils.hpp"
#include "storage/altertable_schema_entry.hpp"
#include "storage/altertable_transaction.hpp"

namespace duckdb {

LogicalType AltertableArrowTypeToLogicalType(const arrow::DataType &arrow_type) {
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
		// DuckDB does not support 256-bit decimals; match scan bind behavior
		return LogicalType::VARCHAR;
	default:
		return LogicalType::VARCHAR;
	}
}


string AltertableUtils::TypeToString(const LogicalType &input) {
	if (input.HasAlias()) {
		if (StringUtil::CIEquals(input.GetAlias(), "wkb_blob")) {
			return "GEOMETRY";
		}
		return input.GetAlias();
	}
	switch (input.id()) {
	case LogicalTypeId::FLOAT:
		return "REAL";
	case LogicalTypeId::DOUBLE:
		return "FLOAT";
	case LogicalTypeId::BLOB:
		return "BYTEA";
	case LogicalTypeId::LIST:
		return AltertableUtils::TypeToString(ListType::GetChildType(input)) + "[]";
	case LogicalTypeId::ENUM:
		throw NotImplementedException("Enums in Altertable must be named - unnamed enums are not supported. Use CREATE "
		                              "TYPE to create a named enum.");
	case LogicalTypeId::STRUCT:
		throw NotImplementedException("Composite types in Altertable must be named - unnamed composite types are not "
		                              "supported. Use CREATE TYPE to create a named composite type.");
	case LogicalTypeId::MAP:
		throw NotImplementedException("MAP type not supported in Altertable");
	case LogicalTypeId::UNION:
		throw NotImplementedException("UNION type not supported in Altertable");
	default:
		return input.ToString();
	}
}

LogicalType GetGeometryType() {
	auto blob_type = LogicalType(LogicalTypeId::BLOB);
	blob_type.SetAlias("WKB_BLOB");
	return blob_type;
}

LogicalType AltertableUtils::RemoveAlias(const LogicalType &type) {
	if (!type.HasAlias()) {
		return type;
	}
	if (StringUtil::CIEquals(type.GetAlias(), "json")) {
		return type;
	}
	if (StringUtil::CIEquals(type.GetAlias(), "geometry")) {
		return GetGeometryType();
	}
	switch (type.id()) {
	case LogicalTypeId::STRUCT: {
		auto child_types = StructType::GetChildTypes(type);
		return LogicalType::STRUCT(std::move(child_types));
	}
	case LogicalTypeId::ENUM: {
		auto &enum_vector = EnumType::GetValuesInsertOrder(type);
		Vector new_vector(LogicalType::VARCHAR);
		new_vector.Reference(enum_vector);
		return LogicalType::ENUM(new_vector, EnumType::GetSize(type));
	}
	default:
		throw InternalException("Unsupported logical type for RemoveAlias");
	}
}

LogicalType AltertableUtils::TypeToLogicalType(optional_ptr<AltertableTransaction> transaction,
                                               optional_ptr<AltertableSchemaEntry> schema,
                                               const AltertableTypeData &type_info, AltertableType &altertable_type) {
	auto &pgtypename = type_info.type_name;

	// altertable array types start with an _
	if (StringUtil::StartsWith(pgtypename, "_")) {
		if (transaction) {
			auto context = transaction->context.lock();
			if (!context) {
				throw InternalException("Context is destroyed!?");
			}
			Value array_as_varchar;
			if (context->TryGetCurrentSetting("altertable_array_as_varchar", array_as_varchar)) {
				if (BooleanValue::Get(array_as_varchar)) {
					altertable_type.info = AltertableTypeAnnotation::CAST_TO_VARCHAR;
					return LogicalType::VARCHAR;
				}
			}
		}
		// get the array dimension information
		idx_t dimensions = type_info.array_dimensions;
		if (dimensions == 0) {
			dimensions = 1;
		}
		// fetch the child type of the array
		AltertableTypeData child_type_info;
		child_type_info.type_name = pgtypename.substr(1);
		child_type_info.type_modifier = type_info.type_modifier;
		AltertableType child_altertable_type;
		auto child_type =
		    AltertableUtils::TypeToLogicalType(transaction, schema, child_type_info, child_altertable_type);
		// construct the child type based on the number of dimensions
		for (idx_t i = 1; i < dimensions; i++) {
			AltertableType new_altertable_type;
			new_altertable_type.children.push_back(std::move(child_altertable_type));
			child_altertable_type = std::move(new_altertable_type);
			child_type = LogicalType::LIST(child_type);
		}
		auto result = LogicalType::LIST(child_type);
		altertable_type.children.push_back(std::move(child_altertable_type));
		return result;
	}

	// Convert type name to uppercase for case-insensitive comparison
	auto type_upper = StringUtil::Upper(pgtypename);

	// DuckDB type names (from information_schema)
	if (type_upper == "BOOLEAN" || pgtypename == "bool") {
		return LogicalType::BOOLEAN;
	} else if (type_upper == "TINYINT" || pgtypename == "int1") {
		return LogicalType::TINYINT;
	} else if (type_upper == "SMALLINT" || pgtypename == "int2") {
		return LogicalType::SMALLINT;
	} else if (type_upper == "INTEGER" || pgtypename == "int4" || type_upper == "INT") {
		return LogicalType::INTEGER;
	} else if (type_upper == "BIGINT" || pgtypename == "int8") {
		return LogicalType::BIGINT;
	} else if (type_upper == "HUGEINT") {
		return LogicalType::HUGEINT;
	} else if (type_upper == "UTINYINT") {
		return LogicalType::UTINYINT;
	} else if (type_upper == "USMALLINT") {
		return LogicalType::USMALLINT;
	} else if (type_upper == "UINTEGER" || pgtypename == "oid") {
		return LogicalType::UINTEGER;
	} else if (type_upper == "UBIGINT") {
		return LogicalType::UBIGINT;
	} else if (type_upper == "FLOAT" || type_upper == "REAL" || pgtypename == "float4") {
		return LogicalType::FLOAT;
	} else if (type_upper == "DOUBLE" || pgtypename == "float8") {
		return LogicalType::DOUBLE;
	} else if (pgtypename == "numeric") {
		auto width = ((type_info.type_modifier - sizeof(int32_t)) >> 16) & 0xffff;
		auto scale = (((type_info.type_modifier - sizeof(int32_t)) & 0x7ff) ^ 1024) - 1024;
		if (type_info.type_modifier == -1 || width < 0 || scale < 0 || width > 38) {
			// fallback to double
			altertable_type.info = AltertableTypeAnnotation::NUMERIC_AS_DOUBLE;
			return LogicalType::DOUBLE;
		}
		return LogicalType::DECIMAL(width, scale);
	} else if (pgtypename == "char" || pgtypename == "bpchar") {
		altertable_type.info = AltertableTypeAnnotation::FIXED_LENGTH_CHAR;
		return LogicalType::VARCHAR;
	} else if (type_upper == "VARCHAR" || pgtypename == "varchar" || pgtypename == "text" || pgtypename == "json") {
		return LogicalType::VARCHAR;
	} else if (pgtypename == "jsonb") {
		altertable_type.info = AltertableTypeAnnotation::JSONB;
		return LogicalType::VARCHAR;
	} else if (pgtypename == "geometry") {
		return GetGeometryType();
	} else if (type_upper == "DATE" || pgtypename == "date") {
		return LogicalType::DATE;
	} else if (type_upper == "BLOB" || pgtypename == "bytea") {
		return LogicalType::BLOB;
	} else if (type_upper == "TIME" || pgtypename == "time") {
		return LogicalType::TIME;
	} else if (type_upper == "TIMETZ" || type_upper == "TIME WITH TIME ZONE" || pgtypename == "timetz") {
		return LogicalType::TIME_TZ;
	} else if (type_upper == "TIMESTAMP" || pgtypename == "timestamp") {
		return LogicalType::TIMESTAMP;
	} else if (type_upper == "TIMESTAMPTZ" || type_upper == "TIMESTAMP WITH TIME ZONE" || pgtypename == "timestamptz") {
		return LogicalType::TIMESTAMP_TZ;
	} else if (type_upper == "INTERVAL" || pgtypename == "interval") {
		return LogicalType::INTERVAL;
	} else if (type_upper == "UUID" || pgtypename == "uuid") {
		return LogicalType::UUID;
	} else if (pgtypename == "point") {
		altertable_type.info = AltertableTypeAnnotation::GEOM_POINT;
		child_list_t<LogicalType> point_struct;
		point_struct.emplace_back(make_pair("x", LogicalType::DOUBLE));
		point_struct.emplace_back(make_pair("y", LogicalType::DOUBLE));
		return LogicalType::STRUCT(point_struct);
	} else if (pgtypename == "line") {
		altertable_type.info = AltertableTypeAnnotation::GEOM_LINE;
		return LogicalType::LIST(LogicalType::DOUBLE);
	} else if (pgtypename == "lseg") {
		altertable_type.info = AltertableTypeAnnotation::GEOM_LINE_SEGMENT;
		return LogicalType::LIST(LogicalType::DOUBLE);
	} else if (pgtypename == "box") {
		altertable_type.info = AltertableTypeAnnotation::GEOM_BOX;
		return LogicalType::LIST(LogicalType::DOUBLE);
	} else if (pgtypename == "path") {
		altertable_type.info = AltertableTypeAnnotation::GEOM_PATH;
		return LogicalType::LIST(LogicalType::DOUBLE);
	} else if (pgtypename == "polygon") {
		altertable_type.info = AltertableTypeAnnotation::GEOM_POLYGON;
		return LogicalType::LIST(LogicalType::DOUBLE);
	} else if (pgtypename == "circle") {
		altertable_type.info = AltertableTypeAnnotation::GEOM_CIRCLE;
		return LogicalType::LIST(LogicalType::DOUBLE);
	} else {
		// unsupported type - fallback to varchar
		altertable_type.info = AltertableTypeAnnotation::CAST_TO_VARCHAR;
		return LogicalType::VARCHAR;
	}
}

LogicalType AltertableUtils::ToAltertableType(const LogicalType &input) {
	switch (input.id()) {
	case LogicalTypeId::BOOLEAN:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::ENUM:
	case LogicalTypeId::BLOB:
	case LogicalTypeId::DATE:
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::INTERVAL:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_TZ:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::UUID:
	case LogicalTypeId::VARCHAR:
		return input;
	case LogicalTypeId::LIST:
		return LogicalType::LIST(ToAltertableType(ListType::GetChildType(input)));
	case LogicalTypeId::STRUCT: {
		child_list_t<LogicalType> new_types;
		for (idx_t c = 0; c < StructType::GetChildCount(input); c++) {
			auto &name = StructType::GetChildName(input, c);
			auto &type = StructType::GetChildType(input, c);
			new_types.push_back(make_pair(name, ToAltertableType(type)));
		}
		auto result = LogicalType::STRUCT(std::move(new_types));
		result.SetAlias(input.GetAlias());
		return result;
	}
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
		return LogicalType::TIMESTAMP;
	case LogicalTypeId::TINYINT:
		return LogicalType::SMALLINT;
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
		return LogicalType::BIGINT;
	case LogicalTypeId::UBIGINT:
		return LogicalType::DECIMAL(20, 0);
	case LogicalTypeId::HUGEINT:
		return LogicalType::DOUBLE;
	default:
		return LogicalType::VARCHAR;
	}
}

AltertableType AltertableUtils::CreateEmptyAltertableType(const LogicalType &type) {
	AltertableType result;
	switch (type.id()) {
	case LogicalTypeId::STRUCT:
		for (auto &child_type : StructType::GetChildTypes(type)) {
			result.children.push_back(CreateEmptyAltertableType(child_type.second));
		}
		break;
	case LogicalTypeId::LIST:
		result.children.push_back(CreateEmptyAltertableType(ListType::GetChildType(type)));
		break;
	default:
		break;
	}
	return result;
}

AltertableVersion AltertableUtils::ExtractAltertableVersion(const string &version_str) {
	AltertableVersion result;
	idx_t pos = 0;
	// scan for the first digit
	while (pos < version_str.size() && !StringUtil::CharacterIsDigit(version_str[pos])) {
		pos++;
	}
	for (idx_t version_idx = 0; version_idx < 3; version_idx++) {
		idx_t digit_start = pos;
		while (pos < version_str.size() && StringUtil::CharacterIsDigit(version_str[pos])) {
			pos++;
		}
		if (digit_start == pos) {
			// no digits
			break;
		}
		// our version is at [digit_start..pos)
		auto digit_str = version_str.substr(digit_start, pos - digit_start);
		auto digit = std::strtoll(digit_str.c_str(), 0, 10);
		switch (version_idx) {
		case 0:
			result.major_v = digit;
			break;
		case 1:
			result.minor_v = digit;
			break;
		default:
			result.patch_v = digit;
			break;
		}

		// check if the next character is a dot, if not we stop
		if (pos >= version_str.size() || version_str[pos] != '.') {
			break;
		}
		pos++;
	}
	return result;
}

string AltertableUtils::QuoteAltertableIdentifier(const string &text) {
	return KeywordHelper::WriteOptionallyQuoted(text, '"', false);
}

} // namespace duckdb
