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

class SetDefaultSpecUpdate {
public:
	static SetDefaultSpecUpdate FromJSON(yyjson_val *obj) {
		SetDefaultSpecUpdate result;

		// Parse BaseUpdate fields
		result.base_update = BaseUpdate::FromJSON(obj);

		auto action_val = yyjson_obj_get(obj, "action");
		if (action_val) {
			result.action = yyjson_get_str(action_val);
		}

		auto spec_id_val = yyjson_obj_get(obj, "spec-id");
		if (spec_id_val) {
			result.spec_id = yyjson_get_sint(spec_id_val);
		}
		else {
			throw IOException("SetDefaultSpecUpdate required property 'spec-id' is missing");
		}

		return result;
	}

public:
	BaseUpdate base_update;
	string action;
	int64_t spec_id;
};
} // namespace rest_api_objects
} // namespace duckdb