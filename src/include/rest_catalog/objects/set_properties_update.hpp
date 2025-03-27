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

class SetPropertiesUpdate {
public:
	static SetPropertiesUpdate FromJSON(yyjson_val *obj) {
		SetPropertiesUpdate result;
		auto action_val = yyjson_obj_get(obj, "action");
		if (action_val) {
			result.action = yyjson_get_str(action_val);
		}
		auto updates_val = yyjson_obj_get(obj, "updates");
		if (updates_val) {
			result.updates = parse_object_of_strings(updates_val);
		}
		return result;
	}
public:
	string action;
	ObjectOfStrings updates;
};

} // namespace rest_api_objects
} // namespace duckdb