#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/base_update.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class RemovePropertiesUpdate {
public:
	static RemovePropertiesUpdate FromJSON(yyjson_val *obj) {
		RemovePropertiesUpdate result;

		// Parse BaseUpdate fields
		result.base_update = BaseUpdate::FromJSON(obj);

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
		} else {
			throw IOException("RemovePropertiesUpdate required property 'removals' is missing");
		}

		return result;
	}

public:
	BaseUpdate base_update;
	string action;
	vector<string> removals;
};
} // namespace rest_api_objects
} // namespace duckdb
