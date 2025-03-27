#pragma once

#include "yyjson.hpp"
#include <string>
#include <vector>
#include <unordered_map>
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class PrimitiveTypeValue {
public:
	static PrimitiveTypeValue FromJSON(yyjson_val *val) {
		PrimitiveTypeValue result;
		if (yyjson_is_bool(val)) {
			result.value_boolean = yyjson_get_bool(val);
			result.has_boolean = true;
		}
		if (yyjson_is_int(val)) {
			result.value_integer = yyjson_get_sint(val);
			result.has_integer = true;
		}
		if (yyjson_is_str(val)) {
			result.value_string = yyjson_get_str(val);
			result.has_string = true;
		}
		return result;
	}

public:
	bool value_boolean;
	bool has_boolean = false;
	int64_t value_integer;
	bool has_integer = false;
	string value_string;
	bool has_string = false;
};
} // namespace rest_api_objects
} // namespace duckdb