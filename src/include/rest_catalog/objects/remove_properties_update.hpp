#pragma once

#include "yyjson.hpp"
#include <string>
#include <vector>
#include <unordered_map>
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/base_update.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class RemovePropertiesUpdate {
public:
	static RemovePropertiesUpdate FromJSON(yyjson_val *obj) {
		RemovePropertiesUpdate result;
		auto action_val = yyjson_obj_get(obj, "action");
		if (action_val) {
			result.action = yyjson_get_str(action_val);
		}
		auto removals_val = yyjson_obj_get(obj, "removals");
		if (removals_val) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(removals_val, idx, max, val) {
				result.removals.push_back(yyjson_get_str(val));
			}
		}
		return result;
	}
public:
	string action;
	vector<string> removals;
};

} // namespace rest_api_objects
} // namespace duckdb