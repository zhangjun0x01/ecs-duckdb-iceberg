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

class RemoveSnapshotsUpdate {
public:
	static RemoveSnapshotsUpdate FromJSON(yyjson_val *obj) {
		RemoveSnapshotsUpdate result;
		auto action_val = yyjson_obj_get(obj, "action");
		if (action_val) {
			result.action = yyjson_get_str(action_val);
		}
		auto snapshot_ids_val = yyjson_obj_get(obj, "snapshot-ids");
		if (snapshot_ids_val) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(snapshot_ids_val, idx, max, val) {
				result.snapshot_ids.push_back(yyjson_get_sint(val));
			}
		}
		return result;
	}
public:
	string action;
	vector<int64_t> snapshot_ids;
};

} // namespace rest_api_objects
} // namespace duckdb