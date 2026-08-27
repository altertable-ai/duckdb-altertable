#include "altertable_utils.hpp"
#include "storage/altertable_schema_entry.hpp"
#include "storage/altertable_transaction.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "arrow/array.h"
#include "arrow/array/array_nested.h"
#include "arrow/extension_type.h"
#include "arrow/util/decimal.h"
#include <cctype>
#include <functional>

namespace duckdb {

static bool NeedsDSNQuoting(const string &value) {
	for (auto c : value) {
		if (StringUtil::CharacterIsSpace(c) || c == '\'' || c == '"' || c == '\\' || c == '=') {
			return true;
		}
	}
	return value.empty();
}

string AltertableUtils::QuoteDSNValue(const string &value) {
	if (!NeedsDSNQuoting(value)) {
		return value;
	}
	string result = "'";
	for (auto c : value) {
		if (c == '\'' || c == '\\') {
			result += '\\';
		}
		result += c;
	}
	result += "'";
	return result;
}

string AltertableUtils::QueryFingerprint(const string &query) {
	return StringUtil::Format("remote SQL (%llu bytes, fingerprint %llu)", query.size(), std::hash<string> {}(query));
}

string AltertableUtils::QualifiedTableReference(const string &catalog, const string &schema, const string &table) {
	string result;
	if (!catalog.empty()) {
		result += QuoteAltertableIdentifier(catalog) + ".";
	}
	return result + QuoteAltertableIdentifier(schema) + "." + QuoteAltertableIdentifier(table);
}

AltertableConnectionConfig AltertableConnectionConfig::Parse(const string &dsn) {
	AltertableConnectionConfig result;
	if (dsn.find('=') == string::npos) {
		idx_t start = 0;
		while (start < dsn.size() && StringUtil::CharacterIsSpace(dsn[start])) {
			start++;
		}
		idx_t end = dsn.size();
		while (end > start && StringUtil::CharacterIsSpace(dsn[end - 1])) {
			end--;
		}
		if (start == end) {
			return result;
		}
		for (idx_t i = start; i < end; i++) {
			if (StringUtil::CharacterIsSpace(dsn[i])) {
				throw InvalidInputException("Invalid ALTERTABLE catalog shorthand: expected a single word");
			}
		}
		result.catalog = dsn.substr(start, end - start);
		result.has_catalog = true;
		return result;
	}
	idx_t pos = 0;
	while (pos < dsn.size()) {
		while (pos < dsn.size() && StringUtil::CharacterIsSpace(dsn[pos])) {
			pos++;
		}
		if (pos >= dsn.size()) {
			break;
		}

		auto key_start = pos;
		while (pos < dsn.size() && dsn[pos] != '=' && !StringUtil::CharacterIsSpace(dsn[pos])) {
			pos++;
		}
		if (key_start == pos) {
			throw InvalidInputException("Invalid ALTERTABLE connection string near position %llu", pos);
		}
		auto key = StringUtil::Lower(dsn.substr(key_start, pos - key_start));
		while (pos < dsn.size() && StringUtil::CharacterIsSpace(dsn[pos])) {
			pos++;
		}
		if (pos >= dsn.size() || dsn[pos] != '=') {
			throw InvalidInputException("Invalid ALTERTABLE connection string: key '%s' has no value", key);
		}
		pos++;
		while (pos < dsn.size() && StringUtil::CharacterIsSpace(dsn[pos])) {
			pos++;
		}

		string value;
		if (pos < dsn.size() && (dsn[pos] == '\'' || dsn[pos] == '"')) {
			auto quote = dsn[pos++];
			bool closed = false;
			while (pos < dsn.size()) {
				auto c = dsn[pos++];
				if (c == '\\' && pos < dsn.size()) {
					value += dsn[pos++];
					continue;
				}
				if (c == quote) {
					closed = true;
					break;
				}
				value += c;
			}
			if (!closed) {
				throw InvalidInputException("Invalid ALTERTABLE connection string: unterminated value for key '%s'",
				                            key);
			}
			if (pos < dsn.size() && !StringUtil::CharacterIsSpace(dsn[pos])) {
				throw InvalidInputException(
				    "Invalid ALTERTABLE connection string: unexpected characters after key '%s'", key);
			}
		} else {
			auto value_start = pos;
			while (pos < dsn.size() && !StringUtil::CharacterIsSpace(dsn[pos])) {
				pos++;
			}
			value = dsn.substr(value_start, pos - value_start);
		}

		if (key == "host") {
			result.host = value;
			result.has_host = true;
		} else if (key == "port") {
			try {
				result.port = std::stoi(value);
			} catch (const std::exception &) {
				throw InvalidInputException("Invalid ALTERTABLE port value '%s'", value);
			}
			if (result.port <= 0 || result.port > 65535) {
				throw InvalidInputException("Invalid ALTERTABLE port value '%s'", value);
			}
			result.has_port = true;
		} else if (key == "user") {
			result.user = value;
			result.has_user = true;
		} else if (key == "password") {
			result.password = value;
			result.has_password = true;
		} else if (key == "ssl") {
			auto lower_value = StringUtil::Lower(value);
			if (lower_value == "false" || lower_value == "0" || lower_value == "no") {
				result.ssl = false;
			} else if (lower_value == "true" || lower_value == "1" || lower_value == "yes" || lower_value.empty()) {
				result.ssl = true;
			} else {
				throw InvalidInputException("Invalid ALTERTABLE ssl value '%s'", value);
			}
			result.has_ssl = true;
		} else if (key == "catalog") {
			result.catalog = value;
			result.has_catalog = true;
		} else if ((key == "database" || key == "dbname") && !result.has_catalog) {
			result.catalog = value;
			result.has_catalog = true;
		} else if (key == "compute_size") {
			result.compute_size = value;
			result.has_compute_size = true;
		} else {
			throw InvalidInputException("Unknown ALTERTABLE connection string key '%s'", key);
		}
	}
	return result;
}

string AltertableConnectionConfig::ToDSN(bool redact_password) const {
	string result;
	auto append = [&](const string &key, const string &value) {
		if (!result.empty()) {
			result += " ";
		}
		result += key + "=" + AltertableUtils::QuoteDSNValue(value);
	};
	if (has_host || host != "flight.altertable.ai") {
		append("host", host);
	}
	if (has_port || port != 443) {
		append("port", to_string(port));
	}
	if (has_user || !user.empty()) {
		append("user", user);
	}
	if (has_password || !password.empty()) {
		append("password", redact_password ? "****" : password);
	}
	if (has_catalog || !catalog.empty()) {
		append("catalog", catalog);
	}
	if (has_compute_size || !compute_size.empty()) {
		append("compute_size", compute_size);
	}
	if (has_ssl || !ssl) {
		append("ssl", ssl ? "true" : "false");
	}
	return result;
}

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
	case arrow::Type::EXTENSION: {
		auto &ext_type = static_cast<const arrow::ExtensionType &>(arrow_type);
		if (ext_type.extension_name() == "arrow.json") {
			return LogicalType::JSON();
		}
		return AltertableArrowTypeToLogicalType(*ext_type.storage_type());
	}
	case arrow::Type::LIST:
	case arrow::Type::LARGE_LIST: {
		auto &list_type = static_cast<const arrow::BaseListType &>(arrow_type);
		return LogicalType::LIST(AltertableArrowTypeToLogicalType(*list_type.value_type()));
	}
	case arrow::Type::MAP: {
		auto &map_type = static_cast<const arrow::MapType &>(arrow_type);
		return LogicalType::MAP(AltertableArrowTypeToLogicalType(*map_type.key_type()),
		                        AltertableArrowTypeToLogicalType(*map_type.item_type()));
	}
	case arrow::Type::STRUCT: {
		auto &struct_type = static_cast<const arrow::StructType &>(arrow_type);
		child_list_t<LogicalType> children;
		for (int i = 0; i < struct_type.num_fields(); i++) {
			auto field = struct_type.field(i);
			children.emplace_back(field->name(), AltertableArrowTypeToLogicalType(*field->type()));
		}
		return LogicalType::STRUCT(std::move(children));
	}
	default:
		return LogicalType::VARCHAR;
	}
}

static void ConvertListArray(Vector &vector, const arrow::Array &array, idx_t offset, idx_t count, bool large) {
	auto list_data = ListVector::GetData(vector);
	idx_t min_start = NumericLimits<idx_t>::Maximum();
	idx_t max_end = 0;
	bool any_valid = false;

	for (idx_t i = 0; i < count; i++) {
		idx_t row = offset + i;
		if (!array.IsValid(static_cast<int64_t>(row))) {
			FlatVector::SetNull(vector, i, true);
			list_data[i].offset = 0;
			list_data[i].length = 0;
			continue;
		}
		int64_t start;
		int64_t length;
		if (large) {
			auto &list_array = static_cast<const arrow::LargeListArray &>(array);
			start = list_array.value_offset(static_cast<int64_t>(row));
			length = list_array.value_length(static_cast<int64_t>(row));
		} else {
			auto &list_array = static_cast<const arrow::ListArray &>(array);
			start = list_array.value_offset(static_cast<int64_t>(row));
			length = list_array.value_length(static_cast<int64_t>(row));
		}
		list_data[i].offset = NumericCast<uint64_t>(start);
		list_data[i].length = NumericCast<uint64_t>(length);
		any_valid = true;
		min_start = MinValue(min_start, NumericCast<idx_t>(start));
		max_end = MaxValue(max_end, NumericCast<idx_t>(start + length));
	}

	if (!any_valid) {
		ListVector::SetListSize(vector, 0);
		return;
	}

	for (idx_t i = 0; i < count; i++) {
		if (!FlatVector::IsNull(vector, i)) {
			list_data[i].offset -= min_start;
		}
	}

	idx_t child_count = max_end - min_start;
	ListVector::Reserve(vector, child_count);
	ListVector::SetListSize(vector, child_count);

	std::shared_ptr<arrow::Array> values;
	if (large) {
		values = static_cast<const arrow::LargeListArray &>(array).values();
	} else {
		values = static_cast<const arrow::ListArray &>(array).values();
	}
	auto sliced = values->Slice(static_cast<int64_t>(min_start), static_cast<int64_t>(child_count));
	AltertableConvertArrowArray(ListVector::GetEntry(vector), *sliced, 0, child_count);
}

static void ConvertMapArray(Vector &vector, const arrow::MapArray &array, idx_t offset, idx_t count) {
	auto list_data = ListVector::GetData(vector);
	idx_t min_start = NumericLimits<idx_t>::Maximum();
	idx_t max_end = 0;
	bool any_valid = false;

	for (idx_t i = 0; i < count; i++) {
		idx_t row = offset + i;
		if (!array.IsValid(static_cast<int64_t>(row))) {
			FlatVector::SetNull(vector, i, true);
			list_data[i].offset = 0;
			list_data[i].length = 0;
			continue;
		}
		auto start = array.value_offset(static_cast<int64_t>(row));
		auto length = array.value_length(static_cast<int64_t>(row));
		list_data[i].offset = NumericCast<uint64_t>(start);
		list_data[i].length = NumericCast<uint64_t>(length);
		any_valid = true;
		min_start = MinValue(min_start, NumericCast<idx_t>(start));
		max_end = MaxValue(max_end, NumericCast<idx_t>(start + length));
	}

	if (!any_valid) {
		ListVector::SetListSize(vector, 0);
		return;
	}

	for (idx_t i = 0; i < count; i++) {
		if (!FlatVector::IsNull(vector, i)) {
			list_data[i].offset -= min_start;
		}
	}

	idx_t child_count = max_end - min_start;
	ListVector::Reserve(vector, child_count);
	ListVector::SetListSize(vector, child_count);

	auto keys = array.keys()->Slice(static_cast<int64_t>(min_start), static_cast<int64_t>(child_count));
	auto items = array.items()->Slice(static_cast<int64_t>(min_start), static_cast<int64_t>(child_count));
	AltertableConvertArrowArray(MapVector::GetKeys(vector), *keys, 0, child_count);
	AltertableConvertArrowArray(MapVector::GetValues(vector), *items, 0, child_count);
}

static void ConvertStructArray(Vector &vector, const arrow::StructArray &array, idx_t offset, idx_t count) {
	for (idx_t i = 0; i < count; i++) {
		if (!array.IsValid(static_cast<int64_t>(offset + i))) {
			FlatVector::SetNull(vector, i, true);
		}
	}
	auto &entries = StructVector::GetEntries(vector);
	idx_t field_count = MinValue<idx_t>(entries.size(), NumericCast<idx_t>(array.num_fields()));
	for (idx_t c = 0; c < field_count; c++) {
		auto field = array.field(static_cast<int>(c));
		AltertableConvertArrowArray(*entries[c], *field, offset, count);
	}
}

void AltertableConvertArrowArray(Vector &vector, const arrow::Array &array, idx_t offset, idx_t count) {
	// GetExecuteSchema can advertise nested columns as VARCHAR while the stream
	// still delivers LIST/MAP arrays. Convert into a typed temporary, then stringify.
	auto mapped_type = AltertableArrowTypeToLogicalType(*array.type());
	if (vector.GetType().id() == LogicalTypeId::VARCHAR && mapped_type.id() != LogicalTypeId::VARCHAR &&
	    mapped_type.id() != LogicalTypeId::BLOB) {
		Vector tmp(mapped_type, count);
		AltertableConvertArrowArray(tmp, array, offset, count);
		auto result = FlatVector::GetData<string_t>(vector);
		for (idx_t i = 0; i < count; i++) {
			if (FlatVector::IsNull(tmp, i)) {
				FlatVector::SetNull(vector, i, true);
			} else {
				result[i] = StringVector::AddString(vector, tmp.GetValue(i).ToString());
			}
		}
		return;
	}

	switch (array.type_id()) {
	case arrow::Type::BOOL: {
		auto &bool_array = static_cast<const arrow::BooleanArray &>(array);
		auto data = FlatVector::GetData<bool>(vector);
		for (idx_t i = 0; i < count; i++) {
			if (bool_array.IsValid(static_cast<int64_t>(offset + i))) {
				data[i] = bool_array.Value(static_cast<int64_t>(offset + i));
			} else {
				FlatVector::SetNull(vector, i, true);
			}
		}
		break;
	}
	case arrow::Type::INT8: {
		auto &int_array = static_cast<const arrow::Int8Array &>(array);
		auto data = FlatVector::GetData<int8_t>(vector);
		for (idx_t i = 0; i < count; i++) {
			if (int_array.IsValid(static_cast<int64_t>(offset + i))) {
				data[i] = int_array.Value(static_cast<int64_t>(offset + i));
			} else {
				FlatVector::SetNull(vector, i, true);
			}
		}
		break;
	}
	case arrow::Type::INT16: {
		auto &int_array = static_cast<const arrow::Int16Array &>(array);
		auto data = FlatVector::GetData<int16_t>(vector);
		for (idx_t i = 0; i < count; i++) {
			if (int_array.IsValid(static_cast<int64_t>(offset + i))) {
				data[i] = int_array.Value(static_cast<int64_t>(offset + i));
			} else {
				FlatVector::SetNull(vector, i, true);
			}
		}
		break;
	}
	case arrow::Type::INT32: {
		auto &int_array = static_cast<const arrow::Int32Array &>(array);
		auto data = FlatVector::GetData<int32_t>(vector);
		for (idx_t i = 0; i < count; i++) {
			if (int_array.IsValid(static_cast<int64_t>(offset + i))) {
				data[i] = int_array.Value(static_cast<int64_t>(offset + i));
			} else {
				FlatVector::SetNull(vector, i, true);
			}
		}
		break;
	}
	case arrow::Type::INT64: {
		auto &int_array = static_cast<const arrow::Int64Array &>(array);
		auto data = FlatVector::GetData<int64_t>(vector);
		for (idx_t i = 0; i < count; i++) {
			if (int_array.IsValid(static_cast<int64_t>(offset + i))) {
				data[i] = int_array.Value(static_cast<int64_t>(offset + i));
			} else {
				FlatVector::SetNull(vector, i, true);
			}
		}
		break;
	}
	case arrow::Type::UINT8: {
		auto &int_array = static_cast<const arrow::UInt8Array &>(array);
		auto data = FlatVector::GetData<uint8_t>(vector);
		for (idx_t i = 0; i < count; i++) {
			if (int_array.IsValid(static_cast<int64_t>(offset + i))) {
				data[i] = int_array.Value(static_cast<int64_t>(offset + i));
			} else {
				FlatVector::SetNull(vector, i, true);
			}
		}
		break;
	}
	case arrow::Type::UINT16: {
		auto &int_array = static_cast<const arrow::UInt16Array &>(array);
		auto data = FlatVector::GetData<uint16_t>(vector);
		for (idx_t i = 0; i < count; i++) {
			if (int_array.IsValid(static_cast<int64_t>(offset + i))) {
				data[i] = int_array.Value(static_cast<int64_t>(offset + i));
			} else {
				FlatVector::SetNull(vector, i, true);
			}
		}
		break;
	}
	case arrow::Type::UINT32: {
		auto &int_array = static_cast<const arrow::UInt32Array &>(array);
		auto data = FlatVector::GetData<uint32_t>(vector);
		for (idx_t i = 0; i < count; i++) {
			if (int_array.IsValid(static_cast<int64_t>(offset + i))) {
				data[i] = int_array.Value(static_cast<int64_t>(offset + i));
			} else {
				FlatVector::SetNull(vector, i, true);
			}
		}
		break;
	}
	case arrow::Type::UINT64: {
		auto &int_array = static_cast<const arrow::UInt64Array &>(array);
		auto data = FlatVector::GetData<uint64_t>(vector);
		for (idx_t i = 0; i < count; i++) {
			if (int_array.IsValid(static_cast<int64_t>(offset + i))) {
				data[i] = int_array.Value(static_cast<int64_t>(offset + i));
			} else {
				FlatVector::SetNull(vector, i, true);
			}
		}
		break;
	}
	case arrow::Type::FLOAT: {
		auto &float_array = static_cast<const arrow::FloatArray &>(array);
		auto data = FlatVector::GetData<float>(vector);
		for (idx_t i = 0; i < count; i++) {
			if (float_array.IsValid(static_cast<int64_t>(offset + i))) {
				data[i] = float_array.Value(static_cast<int64_t>(offset + i));
			} else {
				FlatVector::SetNull(vector, i, true);
			}
		}
		break;
	}
	case arrow::Type::DOUBLE: {
		auto &double_array = static_cast<const arrow::DoubleArray &>(array);
		auto data = FlatVector::GetData<double>(vector);
		for (idx_t i = 0; i < count; i++) {
			if (double_array.IsValid(static_cast<int64_t>(offset + i))) {
				data[i] = double_array.Value(static_cast<int64_t>(offset + i));
			} else {
				FlatVector::SetNull(vector, i, true);
			}
		}
		break;
	}
	case arrow::Type::STRING: {
		auto &str_array = static_cast<const arrow::StringArray &>(array);
		if (vector.GetType() == LogicalType::UUID) {
			auto data = FlatVector::GetData<hugeint_t>(vector);
			for (idx_t i = 0; i < count; i++) {
				if (str_array.IsValid(static_cast<int64_t>(offset + i))) {
					string uuid_str = str_array.GetString(static_cast<int64_t>(offset + i));
					string hex_str;
					for (char c : uuid_str) {
						if (c != '-') {
							hex_str += c;
						}
					}
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
			auto data = FlatVector::GetData<string_t>(vector);
			for (idx_t i = 0; i < count; i++) {
				if (str_array.IsValid(static_cast<int64_t>(offset + i))) {
					string val = str_array.GetString(static_cast<int64_t>(offset + i));
					data[i] = StringVector::AddString(vector, val);
				} else {
					FlatVector::SetNull(vector, i, true);
				}
			}
		}
		break;
	}
	case arrow::Type::LARGE_STRING: {
		auto &str_array = static_cast<const arrow::LargeStringArray &>(array);
		auto data = FlatVector::GetData<string_t>(vector);
		for (idx_t i = 0; i < count; i++) {
			if (str_array.IsValid(static_cast<int64_t>(offset + i))) {
				string val = str_array.GetString(static_cast<int64_t>(offset + i));
				data[i] = StringVector::AddString(vector, val);
			} else {
				FlatVector::SetNull(vector, i, true);
			}
		}
		break;
	}
	case arrow::Type::BINARY: {
		auto &bin_array = static_cast<const arrow::BinaryArray &>(array);
		auto data = FlatVector::GetData<string_t>(vector);
		for (idx_t i = 0; i < count; i++) {
			if (bin_array.IsValid(static_cast<int64_t>(offset + i))) {
				auto view = bin_array.GetView(static_cast<int64_t>(offset + i));
				data[i] = StringVector::AddStringOrBlob(vector, view.data(), view.size());
			} else {
				FlatVector::SetNull(vector, i, true);
			}
		}
		break;
	}
	case arrow::Type::LARGE_BINARY: {
		auto &bin_array = static_cast<const arrow::LargeBinaryArray &>(array);
		auto data = FlatVector::GetData<string_t>(vector);
		for (idx_t i = 0; i < count; i++) {
			if (bin_array.IsValid(static_cast<int64_t>(offset + i))) {
				auto view = bin_array.GetView(static_cast<int64_t>(offset + i));
				data[i] = StringVector::AddStringOrBlob(vector, view.data(), view.size());
			} else {
				FlatVector::SetNull(vector, i, true);
			}
		}
		break;
	}
	case arrow::Type::DATE32: {
		auto &date_array = static_cast<const arrow::Date32Array &>(array);
		auto data = FlatVector::GetData<date_t>(vector);
		for (idx_t i = 0; i < count; i++) {
			if (date_array.IsValid(static_cast<int64_t>(offset + i))) {
				data[i] = date_t(date_array.Value(static_cast<int64_t>(offset + i)));
			} else {
				FlatVector::SetNull(vector, i, true);
			}
		}
		break;
	}
	case arrow::Type::DATE64: {
		auto &date_array = static_cast<const arrow::Date64Array &>(array);
		auto data = FlatVector::GetData<date_t>(vector);
		for (idx_t i = 0; i < count; i++) {
			if (date_array.IsValid(static_cast<int64_t>(offset + i))) {
				int64_t arrow_ms = date_array.Value(static_cast<int64_t>(offset + i));
				data[i] = date_t(static_cast<int32_t>(arrow_ms / (1000 * 60 * 60 * 24)));
			} else {
				FlatVector::SetNull(vector, i, true);
			}
		}
		break;
	}
	case arrow::Type::TIMESTAMP: {
		auto &ts_array = static_cast<const arrow::TimestampArray &>(array);
		auto ts_type = std::static_pointer_cast<arrow::TimestampType>(array.type());
		auto data = FlatVector::GetData<timestamp_t>(vector);
		auto unit = ts_type->unit();
		for (idx_t i = 0; i < count; i++) {
			if (ts_array.IsValid(static_cast<int64_t>(offset + i))) {
				int64_t value = ts_array.Value(static_cast<int64_t>(offset + i));
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
		auto &time_array = static_cast<const arrow::Time32Array &>(array);
		auto time_type = std::static_pointer_cast<arrow::Time32Type>(array.type());
		auto data = FlatVector::GetData<dtime_t>(vector);
		for (idx_t i = 0; i < count; i++) {
			if (time_array.IsValid(static_cast<int64_t>(offset + i))) {
				int32_t value = time_array.Value(static_cast<int64_t>(offset + i));
				int64_t micros = time_type->unit() == arrow::TimeUnit::SECOND ? static_cast<int64_t>(value) * 1000000
				                                                              : static_cast<int64_t>(value) * 1000;
				data[i] = dtime_t(micros);
			} else {
				FlatVector::SetNull(vector, i, true);
			}
		}
		break;
	}
	case arrow::Type::TIME64: {
		auto &time_array = static_cast<const arrow::Time64Array &>(array);
		auto time_type = std::static_pointer_cast<arrow::Time64Type>(array.type());
		auto data = FlatVector::GetData<dtime_t>(vector);
		for (idx_t i = 0; i < count; i++) {
			if (time_array.IsValid(static_cast<int64_t>(offset + i))) {
				int64_t value = time_array.Value(static_cast<int64_t>(offset + i));
				int64_t micros = time_type->unit() == arrow::TimeUnit::MICRO ? value : value / 1000;
				data[i] = dtime_t(micros);
			} else {
				FlatVector::SetNull(vector, i, true);
			}
		}
		break;
	}
	case arrow::Type::FIXED_SIZE_BINARY: {
		auto &fixed_array = static_cast<const arrow::FixedSizeBinaryArray &>(array);
		auto data = FlatVector::GetData<string_t>(vector);
		for (idx_t i = 0; i < count; i++) {
			if (fixed_array.IsValid(static_cast<int64_t>(offset + i))) {
				auto view = fixed_array.GetView(static_cast<int64_t>(offset + i));
				data[i] = StringVector::AddStringOrBlob(vector, view.data(), view.size());
			} else {
				FlatVector::SetNull(vector, i, true);
			}
		}
		break;
	}
	case arrow::Type::DECIMAL128: {
		auto &dec_array = static_cast<const arrow::Decimal128Array &>(array);
		switch (vector.GetType().InternalType()) {
		case PhysicalType::INT16: {
			auto data = FlatVector::GetData<int16_t>(vector);
			for (idx_t i = 0; i < count; i++) {
				if (dec_array.IsValid(static_cast<int64_t>(offset + i))) {
					auto view = dec_array.GetView(static_cast<int64_t>(offset + i));
					arrow::Decimal128 decimal_value(reinterpret_cast<const uint8_t *>(view.data()));
					data[i] = static_cast<int16_t>(decimal_value.low_bits());
				} else {
					FlatVector::SetNull(vector, i, true);
				}
			}
			break;
		}
		case PhysicalType::INT32: {
			auto data = FlatVector::GetData<int32_t>(vector);
			for (idx_t i = 0; i < count; i++) {
				if (dec_array.IsValid(static_cast<int64_t>(offset + i))) {
					auto view = dec_array.GetView(static_cast<int64_t>(offset + i));
					arrow::Decimal128 decimal_value(reinterpret_cast<const uint8_t *>(view.data()));
					data[i] = static_cast<int32_t>(decimal_value.low_bits());
				} else {
					FlatVector::SetNull(vector, i, true);
				}
			}
			break;
		}
		case PhysicalType::INT64: {
			auto data = FlatVector::GetData<int64_t>(vector);
			for (idx_t i = 0; i < count; i++) {
				if (dec_array.IsValid(static_cast<int64_t>(offset + i))) {
					auto view = dec_array.GetView(static_cast<int64_t>(offset + i));
					arrow::Decimal128 decimal_value(reinterpret_cast<const uint8_t *>(view.data()));
					data[i] = static_cast<int64_t>(decimal_value.low_bits());
				} else {
					FlatVector::SetNull(vector, i, true);
				}
			}
			break;
		}
		default: {
			auto data = FlatVector::GetData<hugeint_t>(vector);
			for (idx_t i = 0; i < count; i++) {
				if (dec_array.IsValid(static_cast<int64_t>(offset + i))) {
					auto view = dec_array.GetView(static_cast<int64_t>(offset + i));
					arrow::Decimal128 decimal_value(reinterpret_cast<const uint8_t *>(view.data()));
					auto low = static_cast<uint64_t>(decimal_value.low_bits());
					auto high = static_cast<int64_t>(decimal_value.high_bits());
					data[i] = hugeint_t(high, low);
				} else {
					FlatVector::SetNull(vector, i, true);
				}
			}
			break;
		}
		}
		break;
	}
	case arrow::Type::DECIMAL256: {
		auto &dec_array = static_cast<const arrow::Decimal256Array &>(array);
		auto dec_type = std::static_pointer_cast<arrow::Decimal256Type>(array.type());
		auto data = FlatVector::GetData<string_t>(vector);
		for (idx_t i = 0; i < count; i++) {
			if (dec_array.IsValid(static_cast<int64_t>(offset + i))) {
				auto view = dec_array.GetView(static_cast<int64_t>(offset + i));
				arrow::Decimal256 decimal_value(reinterpret_cast<const uint8_t *>(view.data()));
				data[i] = StringVector::AddString(vector, decimal_value.ToString(dec_type->scale()));
			} else {
				FlatVector::SetNull(vector, i, true);
			}
		}
		break;
	}
	case arrow::Type::EXTENSION: {
		auto &ext_array = static_cast<const arrow::ExtensionArray &>(array);
		AltertableConvertArrowArray(vector, *ext_array.storage(), offset, count);
		break;
	}
	case arrow::Type::LIST:
		ConvertListArray(vector, array, offset, count, false);
		break;
	case arrow::Type::LARGE_LIST:
		ConvertListArray(vector, array, offset, count, true);
		break;
	case arrow::Type::MAP:
		ConvertMapArray(vector, static_cast<const arrow::MapArray &>(array), offset, count);
		break;
	case arrow::Type::STRUCT:
		ConvertStructArray(vector, static_cast<const arrow::StructArray &>(array), offset, count);
		break;
	default:
		throw NotImplementedException("Arrow type %s (ID: %d) not yet supported", array.type()->ToString(),
		                              (int)array.type_id());
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
		return "FLOAT";
	case LogicalTypeId::DOUBLE:
		return "DOUBLE";
	case LogicalTypeId::BLOB:
		return "BLOB";
	case LogicalTypeId::LIST:
		return AltertableUtils::TypeToString(ListType::GetChildType(input)) + "[]";
	case LogicalTypeId::ENUM:
		throw NotImplementedException("Enums in Altertable must be named - unnamed enums are not supported. Use CREATE "
		                              "TYPE to create a named enum.");
	case LogicalTypeId::STRUCT: {
		string result = "STRUCT(";
		auto child_count = StructType::GetChildCount(input);
		bool is_unnamed = StructType::IsUnnamed(input);
		for (idx_t i = 0; i < child_count; i++) {
			if (i > 0) {
				result += ", ";
			}
			if (!is_unnamed) {
				result += QuoteAltertableIdentifier(StructType::GetChildName(input, i));
				result += " ";
			}
			result += TypeToString(StructType::GetChildType(input, i));
		}
		result += ")";
		return result;
	}
	case LogicalTypeId::MAP:
		return "MAP(" + AltertableUtils::TypeToString(MapType::KeyType(input)) + ", " +
		       AltertableUtils::TypeToString(MapType::ValueType(input)) + ")";
	case LogicalTypeId::UNION:
		throw NotImplementedException("UNION type not supported in Altertable");
	default:
		return input.ToString();
	}
}

LogicalType AltertableUtils::RemoveAlias(const LogicalType &type) {
	if (!type.HasAlias()) {
		return type;
	}
	if (StringUtil::CIEquals(type.GetAlias(), "json")) {
		return type;
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

static string UnquoteIdentifier(const string &input) {
	if (input.size() >= 2 && input.front() == '"' && input.back() == '"') {
		string result;
		for (idx_t i = 1; i + 1 < input.size(); i++) {
			if (input[i] == '"' && i + 1 < input.size() - 1 && input[i + 1] == '"') {
				result.push_back('"');
				i++;
			} else {
				result.push_back(input[i]);
			}
		}
		return result;
	}
	return input;
}

static LogicalType UnquoteNestedTypeNames(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::STRUCT: {
		child_list_t<LogicalType> children;
		for (idx_t i = 0; i < StructType::GetChildCount(type); i++) {
			children.emplace_back(UnquoteIdentifier(StructType::GetChildName(type, i)),
			                      UnquoteNestedTypeNames(StructType::GetChildType(type, i)));
		}
		return LogicalType::STRUCT(std::move(children));
	}
	case LogicalTypeId::LIST:
		return LogicalType::LIST(UnquoteNestedTypeNames(ListType::GetChildType(type)));
	case LogicalTypeId::MAP:
		return LogicalType::MAP(UnquoteNestedTypeNames(MapType::KeyType(type)),
		                        UnquoteNestedTypeNames(MapType::ValueType(type)));
	default:
		return type;
	}
}

LogicalType AltertableUtils::TypeToLogicalType(const AltertableTypeData &type_info) {
	auto &type_name = type_info.type_name;
	auto type_upper = StringUtil::Upper(type_name);

	if (type_upper == "BOOLEAN") {
		return LogicalType::BOOLEAN;
	} else if (type_upper == "TINYINT") {
		return LogicalType::TINYINT;
	} else if (type_upper == "SMALLINT") {
		return LogicalType::SMALLINT;
	} else if (type_upper == "INTEGER" || type_upper == "INT") {
		return LogicalType::INTEGER;
	} else if (type_upper == "BIGINT") {
		return LogicalType::BIGINT;
	} else if (type_upper == "HUGEINT") {
		return LogicalType::HUGEINT;
	} else if (type_upper == "UTINYINT") {
		return LogicalType::UTINYINT;
	} else if (type_upper == "USMALLINT") {
		return LogicalType::USMALLINT;
	} else if (type_upper == "UINTEGER") {
		return LogicalType::UINTEGER;
	} else if (type_upper == "UBIGINT") {
		return LogicalType::UBIGINT;
	} else if (type_upper == "FLOAT" || type_upper == "REAL") {
		return LogicalType::FLOAT;
	} else if (type_upper == "DOUBLE") {
		return LogicalType::DOUBLE;
	} else if (StringUtil::StartsWith(type_upper, "DECIMAL")) {
		if (type_info.numeric_precision > 0) {
			return LogicalType::DECIMAL(type_info.numeric_precision, type_info.numeric_scale);
		}
		return LogicalType::DOUBLE;
	} else if (type_upper == "JSON") {
		return LogicalType::JSON();
	} else if (type_upper == "VARCHAR") {
		return LogicalType::VARCHAR;
	} else if (type_upper == "DATE") {
		return LogicalType::DATE;
	} else if (type_upper == "BLOB") {
		return LogicalType::BLOB;
	} else if (type_upper == "TIME") {
		return LogicalType::TIME;
	} else if (type_upper == "TIMETZ" || type_upper == "TIME WITH TIME ZONE") {
		return LogicalType::TIME_TZ;
	} else if (type_upper == "TIMESTAMP") {
		return LogicalType::TIMESTAMP;
	} else if (type_upper == "TIMESTAMPTZ" || type_upper == "TIMESTAMP WITH TIME ZONE") {
		return LogicalType::TIMESTAMP_TZ;
	} else if (type_upper == "INTERVAL") {
		return LogicalType::INTERVAL;
	} else if (type_upper == "UUID") {
		return LogicalType::UUID;
	} else if (StringUtil::StartsWith(type_upper, "STRUCT(") || StringUtil::StartsWith(type_upper, "MAP(") ||
	           StringUtil::EndsWith(type_upper, "[]")) {
		return UnquoteNestedTypeNames(DBConfig::ParseLogicalType(type_name));
	} else {
		return LogicalType::VARCHAR;
	}
}

LogicalType AltertableUtils::ToAltertableType(const LogicalType &input) {
	switch (input.id()) {
	case LogicalTypeId::BOOLEAN:
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
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
	case LogicalTypeId::MAP:
		return LogicalType::MAP(ToAltertableType(MapType::KeyType(input)), ToAltertableType(MapType::ValueType(input)));
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
	default:
		return LogicalType::VARCHAR;
	}
}

string AltertableUtils::QuoteAltertableIdentifier(const string &text) {
	return KeywordHelper::WriteOptionallyQuoted(text, '"', false);
}

bool TryGetAltertableComparisonOperator(ExpressionType type, string &result) {
	switch (type) {
	case ExpressionType::COMPARE_EQUAL:
		result = "=";
		return true;
	case ExpressionType::COMPARE_NOTEQUAL:
		result = "<>";
		return true;
	case ExpressionType::COMPARE_LESSTHAN:
		result = "<";
		return true;
	case ExpressionType::COMPARE_GREATERTHAN:
		result = ">";
		return true;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		result = "<=";
		return true;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		result = ">=";
		return true;
	case ExpressionType::COMPARE_DISTINCT_FROM:
		result = "IS DISTINCT FROM";
		return true;
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		result = "IS NOT DISTINCT FROM";
		return true;
	default:
		return false;
	}
}

bool TryGetAltertablePredicate(TableFilter &filter, const string &column_name, string &predicate) {
	auto render_conjunction = [&](auto &child_filters, const string &conjunction_operator) {
		string result;
		for (auto &child_filter : child_filters) {
			string child_predicate;
			if (!TryGetAltertablePredicate(*child_filter, column_name, child_predicate)) {
				return false;
			}
			if (child_predicate.empty()) {
				continue;
			}
			if (!result.empty()) {
				result += conjunction_operator;
			}
			result += "(" + child_predicate + ")";
		}
		predicate = std::move(result);
		return true;
	};

	switch (filter.filter_type) {
	case TableFilterType::CONSTANT_COMPARISON: {
		auto &constant_filter = filter.Cast<ConstantFilter>();
		string comparison_operator;
		if (!TryGetAltertableComparisonOperator(constant_filter.comparison_type, comparison_operator)) {
			return false;
		}
		predicate = AltertableUtils::QuoteAltertableIdentifier(column_name) + " " + comparison_operator + " " +
		            constant_filter.constant.ToSQLString();
		return true;
	}
	case TableFilterType::CONJUNCTION_AND:
		return render_conjunction(filter.Cast<ConjunctionAndFilter>().child_filters, " AND ");
	case TableFilterType::CONJUNCTION_OR:
		return render_conjunction(filter.Cast<ConjunctionOrFilter>().child_filters, " OR ");
	case TableFilterType::OPTIONAL_FILTER: {
		auto &optional_filter = filter.Cast<OptionalFilter>();
		if (!optional_filter.child_filter) {
			predicate.clear();
			return true;
		}
		if (!TryGetAltertablePredicate(*optional_filter.child_filter, column_name, predicate)) {
			predicate.clear();
		}
		return true;
	}
	case TableFilterType::IS_NULL:
		predicate = AltertableUtils::QuoteAltertableIdentifier(column_name) + " IS NULL";
		return true;
	case TableFilterType::IS_NOT_NULL:
		predicate = AltertableUtils::QuoteAltertableIdentifier(column_name) + " IS NOT NULL";
		return true;
	default:
		return false;
	}
}

} // namespace duckdb
