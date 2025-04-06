
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

class SetPropertiesUpdate {
public:
	SetPropertiesUpdate() {
	}

public:
	static SetPropertiesUpdate FromJSON(yyjson_val *obj) {
		SetPropertiesUpdate res;
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

		auto updates_val = yyjson_obj_get(obj, "updates");
		if (!updates_val) {
			return "SetPropertiesUpdate required property 'updates' is missing";
		} else {
			updates = parse_object_of_strings(updates_val);
		}

		auto action_val = yyjson_obj_get(obj, "action");
		if (action_val) {
			action = yyjson_get_str(action_val);
		}

		return string();
	}

public:
	BaseUpdate base_update;

public:
	string action;
	case_insensitive_map_t<string> updates;
};

} // namespace rest_api_objects
} // namespace duckdb
