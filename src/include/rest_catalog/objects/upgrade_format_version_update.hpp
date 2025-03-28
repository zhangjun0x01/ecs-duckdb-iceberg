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

class UpgradeFormatVersionUpdate {
public:
	static UpgradeFormatVersionUpdate FromJSON(yyjson_val *obj) {
		UpgradeFormatVersionUpdate result;

		// Parse BaseUpdate fields
		result.base_update = BaseUpdate::FromJSON(obj);

		auto action_val = yyjson_obj_get(obj, "action");
		if (action_val) {
			result.action = yyjson_get_str(action_val);
		}

		auto format_version_val = yyjson_obj_get(obj, "format-version");
		if (format_version_val) {
			result.format_version = yyjson_get_sint(format_version_val);
		}
		else {
			throw IOException("UpgradeFormatVersionUpdate required property 'format-version' is missing");
		}

		return result;
	}

public:
	BaseUpdate base_update;
	string action;
	int64_t format_version;
};
} // namespace rest_api_objects
} // namespace duckdb