
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

class RemoveSnapshotsUpdate {
public:
	RemoveSnapshotsUpdate() {
	}

public:
	static RemoveSnapshotsUpdate FromJSON(yyjson_val *obj) {
		RemoveSnapshotsUpdate res;
		auto error = res.TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return res;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;
		error = base_update.TryFromJSON(obj);
		if (!error.empty()) {
			return error;
		}
		auto snapshot_ids_val = yyjson_obj_get(obj, "snapshot_ids");
		if (!snapshot_ids_val) {
			return "RemoveSnapshotsUpdate required property 'snapshot_ids' is missing";
		} else {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(snapshot_ids_val, idx, max, val) {
				auto tmp = yyjson_get_sint(val);
				snapshot_ids.push_back(tmp);
			}
		}
		auto action_val = yyjson_obj_get(obj, "action");
		if (action_val) {
			action = yyjson_get_str(action_val);
		}
		return string();
	}

public:
	BaseUpdate base_update;
	vector<int64_t> snapshot_ids;
	string action;
};

} // namespace rest_api_objects
} // namespace duckdb
