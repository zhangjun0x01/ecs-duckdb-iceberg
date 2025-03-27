#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class BaseUpdate {
public:
	static BaseUpdate FromJSON(yyjson_val *obj) {
		BaseUpdate result;

		auto action_val = yyjson_obj_get(obj, "action");
		if (action_val) {
			result.action = yyjson_get_str(action_val);
		}
		else {
			throw IOException("BaseUpdate required property 'action' is missing");
		}

		return result;
	}

public:
	string action;
};
} // namespace rest_api_objects
} // namespace duckdb