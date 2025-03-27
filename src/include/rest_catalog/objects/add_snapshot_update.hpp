#pragma once

#include "yyjson.hpp"
#include <string>
#include <vector>
#include <unordered_map>
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/base_update.hpp"
#include "rest_catalog/objects/snapshot.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class AddSnapshotUpdate {
public:
	static AddSnapshotUpdate FromJSON(yyjson_val *obj) {
		AddSnapshotUpdate result;
		auto action_val = yyjson_obj_get(obj, "action");
		if (action_val) {
			result.action = yyjson_get_str(action_val);
		}
		auto snapshot_val = yyjson_obj_get(obj, "snapshot");
		if (snapshot_val) {
			result.snapshot = Snapshot::FromJSON(snapshot_val);
		}
		return result;
	}
public:
	string action;
	Snapshot snapshot;
};

} // namespace rest_api_objects
} // namespace duckdb