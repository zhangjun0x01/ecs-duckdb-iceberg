
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/binary_type_value.hpp"
#include "rest_catalog/objects/boolean_type_value.hpp"
#include "rest_catalog/objects/date_type_value.hpp"
#include "rest_catalog/objects/decimal_type_value.hpp"
#include "rest_catalog/objects/double_type_value.hpp"
#include "rest_catalog/objects/fixed_type_value.hpp"
#include "rest_catalog/objects/float_type_value.hpp"
#include "rest_catalog/objects/integer_type_value.hpp"
#include "rest_catalog/objects/long_type_value.hpp"
#include "rest_catalog/objects/string_type_value.hpp"
#include "rest_catalog/objects/time_type_value.hpp"
#include "rest_catalog/objects/timestamp_nano_type_value.hpp"
#include "rest_catalog/objects/timestamp_type_value.hpp"
#include "rest_catalog/objects/timestamp_tz_nano_type_value.hpp"
#include "rest_catalog/objects/timestamp_tz_type_value.hpp"
#include "rest_catalog/objects/uuidtype_value.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class PrimitiveTypeValue {
public:
	PrimitiveTypeValue() {
	}

public:
	static PrimitiveTypeValue FromJSON(yyjson_val *obj) {
		PrimitiveTypeValue res;
		auto error = res.TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return res;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;
		do {
			error = boolean_type_value.TryFromJSON(obj);
			if (error.empty()) {
				has_boolean_type_value = true;
				break;
			}
			error = integer_type_value.TryFromJSON(obj);
			if (error.empty()) {
				has_integer_type_value = true;
				break;
			}
			error = long_type_value.TryFromJSON(obj);
			if (error.empty()) {
				has_long_type_value = true;
				break;
			}
			error = float_type_value.TryFromJSON(obj);
			if (error.empty()) {
				has_float_type_value = true;
				break;
			}
			error = double_type_value.TryFromJSON(obj);
			if (error.empty()) {
				has_double_type_value = true;
				break;
			}
			error = decimal_type_value.TryFromJSON(obj);
			if (error.empty()) {
				has_decimal_type_value = true;
				break;
			}
			error = string_type_value.TryFromJSON(obj);
			if (error.empty()) {
				has_string_type_value = true;
				break;
			}
			error = uuidtype_value.TryFromJSON(obj);
			if (error.empty()) {
				has_uuidtype_value = true;
				break;
			}
			error = date_type_value.TryFromJSON(obj);
			if (error.empty()) {
				has_date_type_value = true;
				break;
			}
			error = time_type_value.TryFromJSON(obj);
			if (error.empty()) {
				has_time_type_value = true;
				break;
			}
			error = timestamp_type_value.TryFromJSON(obj);
			if (error.empty()) {
				has_timestamp_type_value = true;
				break;
			}
			error = timestamp_tz_type_value.TryFromJSON(obj);
			if (error.empty()) {
				has_timestamp_tz_type_value = true;
				break;
			}
			error = timestamp_nano_type_value.TryFromJSON(obj);
			if (error.empty()) {
				has_timestamp_nano_type_value = true;
				break;
			}
			error = timestamp_tz_nano_type_value.TryFromJSON(obj);
			if (error.empty()) {
				has_timestamp_tz_nano_type_value = true;
				break;
			}
			error = fixed_type_value.TryFromJSON(obj);
			if (error.empty()) {
				has_fixed_type_value = true;
				break;
			}
			error = binary_type_value.TryFromJSON(obj);
			if (error.empty()) {
				has_binary_type_value = true;
				break;
			}
			return "PrimitiveTypeValue failed to parse, none of the oneOf candidates matched";
		} while (false);
		return string();
	}

public:
	BooleanTypeValue boolean_type_value;
	bool has_boolean_type_value = false;
	IntegerTypeValue integer_type_value;
	bool has_integer_type_value = false;
	LongTypeValue long_type_value;
	bool has_long_type_value = false;
	FloatTypeValue float_type_value;
	bool has_float_type_value = false;
	DoubleTypeValue double_type_value;
	bool has_double_type_value = false;
	DecimalTypeValue decimal_type_value;
	bool has_decimal_type_value = false;
	StringTypeValue string_type_value;
	bool has_string_type_value = false;
	UUIDTypeValue uuidtype_value;
	bool has_uuidtype_value = false;
	DateTypeValue date_type_value;
	bool has_date_type_value = false;
	TimeTypeValue time_type_value;
	bool has_time_type_value = false;
	TimestampTypeValue timestamp_type_value;
	bool has_timestamp_type_value = false;
	TimestampTzTypeValue timestamp_tz_type_value;
	bool has_timestamp_tz_type_value = false;
	TimestampNanoTypeValue timestamp_nano_type_value;
	bool has_timestamp_nano_type_value = false;
	TimestampTzNanoTypeValue timestamp_tz_nano_type_value;
	bool has_timestamp_tz_nano_type_value = false;
	FixedTypeValue fixed_type_value;
	bool has_fixed_type_value = false;
	BinaryTypeValue binary_type_value;
	bool has_binary_type_value = false;
};

} // namespace rest_api_objects
} // namespace duckdb
