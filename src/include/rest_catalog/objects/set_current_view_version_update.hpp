
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

class SetCurrentViewVersionUpdate {
public:
	SetCurrentViewVersionUpdate() {
	}

public:
	static SetCurrentViewVersionUpdate FromJSON(yyjson_val *obj) {
		SetCurrentViewVersionUpdate res;
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

		auto view_version_id_val = yyjson_obj_get(obj, "view_version_id");
		if (!view_version_id_val) {
			return "SetCurrentViewVersionUpdate required property 'view_version_id' is missing";
		} else {
			view_version_id = yyjson_get_sint(view_version_id_val);
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
	int64_t view_version_id;
};

} // namespace rest_api_objects
} // namespace duckdb
