#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/base_update.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class SetPropertiesUpdate {
public:
	static SetPropertiesUpdate FromJSON(yyjson_val *obj) {
		SetPropertiesUpdate result;

		// Parse BaseUpdate fields
		result.base_update = BaseUpdate::FromJSON(obj);

		auto action_val = yyjson_obj_get(obj, "action");
		if (action_val) {
			result.action = yyjson_get_str(action_val);
		}

		auto updates_val = yyjson_obj_get(obj, "updates");
		if (updates_val) {
			result.updates = parse_object_of_strings(updates_val);
		}
		else {
			throw IOException("SetPropertiesUpdate required property 'updates' is missing");
		}

		return result;
	}

public:
	BaseUpdate base_update;
	string action;
	ObjectOfStrings updates;
};
} // namespace rest_api_objects
} // namespace duckdb