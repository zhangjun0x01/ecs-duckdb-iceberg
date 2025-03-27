#pragma once

#include "yyjson.hpp"
#include <string>
#include <vector>
#include <unordered_map>
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class Term {
public:
	static Term FromJSON(yyjson_val *val) {
		Term result;
		if (yyjson_is_str(val)) {
			result.value_string = yyjson_get_str(val);
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