#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/base_update.hpp"
#include "rest_catalog/objects/snapshot_reference.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class SetSnapshotRefUpdate {
public:
	static SetSnapshotRefUpdate FromJSON(yyjson_val *obj) {
		SetSnapshotRefUpdate result;

		// Parse BaseUpdate fields
		result.base_update = BaseUpdate::FromJSON(obj);

		// Parse SnapshotReference fields
		result.snapshot_reference = SnapshotReference::FromJSON(obj);

		auto action_val = yyjson_obj_get(obj, "action");
		if (action_val) {
			result.action = yyjson_get_str(action_val);
		}

		auto ref_name_val = yyjson_obj_get(obj, "ref-name");
		if (ref_name_val) {
			result.ref_name = yyjson_get_str(ref_name_val);
		} else {
			throw IOException("SetSnapshotRefUpdate required property 'ref-name' is missing");
		}

		return result;
	}

public:
	BaseUpdate base_update;
	SnapshotReference snapshot_reference;
	string action;
	string ref_name;
};
} // namespace rest_api_objects
} // namespace duckdb
