#pragma once

#include "yyjson.hpp"
#include <string>
#include <vector>
#include <unordered_map>
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class TableRequirement {
public:
	static TableRequirement FromJSON(yyjson_val *obj) {
		TableRequirement result;

		auto type_val = yyjson_obj_get(obj, "type");
		if (type_val) {
			result.type = yyjson_get_str(type_val);
		}
		else {
			throw IOException("TableRequirement required property 'type' is missing");
		}

		return result;
	}

public:
	string type;
};
} // namespace rest_api_objects
} // namespace duckdb