#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class Type {
public:
	static Type FromJSON(yyjson_val *obj) {
		Type result;
		if (yyjson_is_str(obj)) {
			result.value_string = yyjson_get_str(obj);
			result.has_string = true;
		}
		return result;
	}

public:
	string value_string;
	bool has_string = false;
};
} // namespace rest_api_objects
} // namespace duckdb