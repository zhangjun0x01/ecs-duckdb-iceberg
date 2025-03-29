#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class PrimitiveTypeValue {
public:
	static PrimitiveTypeValue FromJSON(yyjson_val *obj) {
		PrimitiveTypeValue result;
		if (yyjson_is_bool(obj)) {
			result.value_boolean = yyjson_get_bool(obj);
			result.has_boolean = true;
		}
		if (yyjson_is_int(obj)) {
			result.value_integer = yyjson_get_sint(obj);
			result.has_integer = true;
		}
		if (yyjson_is_int(obj)) {
			result.value_int64 = yyjson_get_sint(obj);
			result.has_int64 = true;
		}
		if (yyjson_is_num(obj)) {
			result.value_float = yyjson_get_real(obj);
			result.has_float = true;
		}
		if (yyjson_is_num(obj)) {
			result.value_double = yyjson_get_real(obj);
			result.has_double = true;
		}
		if (yyjson_is_str(obj)) {
			result.value_string = yyjson_get_str(obj);
			result.has_string = true;
		}
		if (yyjson_is_str(obj)) {
			result.value_string = yyjson_get_str(obj);
			result.has_string = true;
		}
		if (yyjson_is_str(obj)) {
			result.value_uuid = yyjson_get_str(obj);
			result.has_uuid = true;
		}
		if (yyjson_is_str(obj)) {
			result.value_date = yyjson_get_str(obj);
			result.has_date = true;
		}
		if (yyjson_is_str(obj)) {
			result.value_string = yyjson_get_str(obj);
			result.has_string = true;
		}
		if (yyjson_is_str(obj)) {
			result.value_string = yyjson_get_str(obj);
			result.has_string = true;
		}
		if (yyjson_is_str(obj)) {
			result.value_string = yyjson_get_str(obj);
			result.has_string = true;
		}
		if (yyjson_is_str(obj)) {
			result.value_string = yyjson_get_str(obj);
			result.has_string = true;
		}
		if (yyjson_is_str(obj)) {
			result.value_string = yyjson_get_str(obj);
			result.has_string = true;
		}
		if (yyjson_is_str(obj)) {
			result.value_string = yyjson_get_str(obj);
			result.has_string = true;
		}
		if (yyjson_is_str(obj)) {
			result.value_string = yyjson_get_str(obj);
			result.has_string = true;
		}
		return result;
	}

public:
	bool value_boolean;
	bool has_boolean = false;
	int64_t value_integer;
	bool has_integer = false;
	int64_t value_int64;
	bool has_int64 = false;
	float value_float;
	bool has_float = false;
	double value_double;
	bool has_double = false;
	string value_string;
	bool has_string = false;
	string value_string;
	bool has_string = false;
	string value_uuid;
	bool has_uuid = false;
	string value_date;
	bool has_date = false;
	string value_string;
	bool has_string = false;
	string value_string;
	bool has_string = false;
	string value_string;
	bool has_string = false;
	string value_string;
	bool has_string = false;
	string value_string;
	bool has_string = false;
	string value_string;
	bool has_string = false;
	string value_string;
	bool has_string = false;
};
} // namespace rest_api_objects
} // namespace duckdb
