
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
	AddViewVersionUpdate::AddViewVersionUpdate() {
	}

public:
	static AddViewVersionUpdate FromJSON(yyjson_val *obj) {
		auto error = TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return *this;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		error = base_base_update.TryFromJSON(obj);
		if (!error.empty()) {
			return error;
		}

		auto view_version_val = yyjson_obj_get(obj, "view_version");
		if (!view_version_val) {
		return "AddViewVersionUpdate required property 'view_version' is missing");
		}
		result.view_version = ViewVersion::FromJSON(view_version_val);

		auto action_val = yyjson_obj_get(obj, "action");
		if (action_val) {
			result.action = yyjson_get_str(action_val);
		}
		return string();
	}

public:
	BaseUpdate base_update;

public:
};

} // namespace rest_api_objects
} // namespace duckdb
