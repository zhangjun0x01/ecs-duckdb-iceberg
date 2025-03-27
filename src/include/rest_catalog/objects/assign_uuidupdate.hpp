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

class AssignUUIDUpdate {
public:
	static AssignUUIDUpdate FromJSON(yyjson_val *obj) {
		AssignUUIDUpdate result;

		// Parse BaseUpdate fields
		result.base_update = BaseUpdate::FromJSON(obj);

		auto action_val = yyjson_obj_get(obj, "action");
		if (action_val) {
			result.action = yyjson_get_str(action_val);
		}

		auto uuid_val = yyjson_obj_get(obj, "uuid");
		if (uuid_val) {
			result.uuid = yyjson_get_str(uuid_val);
		}
		else {
			throw IOException("AssignUUIDUpdate required property 'uuid' is missing");
		}

		return result;
	}

public:
	BaseUpdate base_update;
	string action;
	string uuid;
};
} // namespace rest_api_objects
} // namespace duckdb