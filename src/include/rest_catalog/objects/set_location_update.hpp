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

class SetLocationUpdate {
public:
	static SetLocationUpdate FromJSON(yyjson_val *obj) {
		SetLocationUpdate result;

		// Parse BaseUpdate fields
		result.base_update = BaseUpdate::FromJSON(obj);

		auto action_val = yyjson_obj_get(obj, "action");
		if (action_val) {
			result.action = yyjson_get_str(action_val);
		}

		auto location_val = yyjson_obj_get(obj, "location");
		if (location_val) {
			result.location = yyjson_get_str(location_val);
		}
		else {
			throw IOException("SetLocationUpdate required property 'location' is missing");
		}

		return result;
	}

public:
	BaseUpdate base_update;
	string action;
	string location;
};
} // namespace rest_api_objects
} // namespace duckdb