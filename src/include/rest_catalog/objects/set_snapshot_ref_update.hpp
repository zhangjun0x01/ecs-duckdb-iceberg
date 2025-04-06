
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
	SetSnapshotRefUpdate() {
	}

public:
	static SetSnapshotRefUpdate FromJSON(yyjson_val *obj) {
		SetSnapshotRefUpdate res;
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
		error = snapshot_reference.TryFromJSON(obj);
		if (!error.empty()) {
			return error;
		}
		auto ref_name_val = yyjson_obj_get(obj, "ref_name");
		if (!ref_name_val) {
			return "SetSnapshotRefUpdate required property 'ref_name' is missing";
		} else {
			ref_name = yyjson_get_str(ref_name_val);
		}
		auto action_val = yyjson_obj_get(obj, "action");
		if (action_val) {
			action = yyjson_get_str(action_val);
		}
		return string();
	}

public:
	BaseUpdate base_update;
	SnapshotReference snapshot_reference;
	string ref_name;
	string action;
};

} // namespace rest_api_objects
} // namespace duckdb
