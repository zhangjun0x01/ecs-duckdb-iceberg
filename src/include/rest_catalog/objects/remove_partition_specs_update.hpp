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

class RemovePartitionSpecsUpdate {
public:
	static RemovePartitionSpecsUpdate FromJSON(yyjson_val *obj) {
		RemovePartitionSpecsUpdate result;
		auto action_val = yyjson_obj_get(obj, "action");
		if (action_val) {
			result.action = yyjson_get_str(action_val);
		}
		auto spec_ids_val = yyjson_obj_get(obj, "spec-ids");
		if (spec_ids_val) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(spec_ids_val, idx, max, val) {
				result.spec_ids.push_back(yyjson_get_sint(val));
			}
		}
		return result;
	}
public:
	string action;
	vector<int64_t> spec_ids;
};

} // namespace rest_api_objects
} // namespace duckdb