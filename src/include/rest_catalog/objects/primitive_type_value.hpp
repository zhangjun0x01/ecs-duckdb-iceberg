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
		if (yyjson_is_real(obj)) {
			result.value_number = yyjson_get_real(obj);
			result.has_number = true;
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
	double value_number;
	bool has_number = false;
	string value_string;
	bool has_string = false;
};
} // namespace rest_api_objects
} // namespace duckdb
