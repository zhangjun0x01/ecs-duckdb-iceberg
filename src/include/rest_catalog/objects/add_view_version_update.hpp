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
	static AddViewVersionUpdate FromJSON(yyjson_val *obj) {
		AddViewVersionUpdate result;

		// Parse BaseUpdate fields
		result.base_update = BaseUpdate::FromJSON(obj);

		auto action_val = yyjson_obj_get(obj, "action");
		if (action_val) {
			result.action = yyjson_get_str(action_val);
		}

		auto view_version_val = yyjson_obj_get(obj, "view-version");
		if (view_version_val) {
			result.view_version = ViewVersion::FromJSON(view_version_val);
		}
		else {
			throw IOException("AddViewVersionUpdate required property 'view-version' is missing");
		}

		return result;
	}

public:
	BaseUpdate base_update;
	string action;
	ViewVersion view_version;
};
} // namespace rest_api_objects
} // namespace duckdb