
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

class AssignUUIDUpdate {
public:
	AssignUUIDUpdate() {
	}

public:
	static AssignUUIDUpdate FromJSON(yyjson_val *obj) {
		AssignUUIDUpdate res;
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
		auto uuid_val = yyjson_obj_get(obj, "uuid");
		if (!uuid_val) {
			return "AssignUUIDUpdate required property 'uuid' is missing";
		} else {
			uuid = yyjson_get_str(uuid_val);
		}
		auto action_val = yyjson_obj_get(obj, "action");
		if (action_val) {
			action = yyjson_get_str(action_val);
		}
		return string();
	}

public:
	BaseUpdate base_update;
	string uuid;
	string action;
};

} // namespace rest_api_objects
} // namespace duckdb
