
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/base_update.hpp"
#include "rest_catalog/objects/view_version.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class AddViewVersionUpdate {
public:
	AddViewVersionUpdate() {
	}

public:
	static AddViewVersionUpdate FromJSON(yyjson_val *obj) {
		AddViewVersionUpdate res;
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

		auto view_version_val = yyjson_obj_get(obj, "view_version");
		if (!view_version_val) {
			return "AddViewVersionUpdate required property 'view_version' is missing";
		} else {
			error = view_version.TryFromJSON(view_version_val);
			if (!error.empty()) {
				return error;
			}
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
	ViewVersion view_version;
};

} // namespace rest_api_objects
} // namespace duckdb
