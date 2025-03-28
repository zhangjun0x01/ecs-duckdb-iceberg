#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
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

		// Parse BaseUpdate fields
		result.base_update = BaseUpdate::FromJSON(obj);

		auto action_val = yyjson_obj_get(obj, "action");
		if (action_val) {
			result.action = yyjson_get_str(action_val);
		}

		auto snapshot_val = yyjson_obj_get(obj, "snapshot");
		if (snapshot_val) {
			result.snapshot = Snapshot::FromJSON(snapshot_val);
		}
		else {
			throw IOException("AddSnapshotUpdate required property 'snapshot' is missing");
		}

		return result;
	}

public:
	BaseUpdate base_update;
	string action;
	Snapshot snapshot;
};
} // namespace rest_api_objects
} // namespace duckdb