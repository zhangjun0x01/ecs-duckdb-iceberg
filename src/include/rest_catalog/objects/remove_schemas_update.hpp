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

class RemoveSchemasUpdate {
public:
	static RemoveSchemasUpdate FromJSON(yyjson_val *obj) {
		RemoveSchemasUpdate result;

		// Parse BaseUpdate fields
		result.base_update = BaseUpdate::FromJSON(obj);

		auto action_val = yyjson_obj_get(obj, "action");
		if (action_val) {
			result.action = yyjson_get_str(action_val);
		}

		auto schema_ids_val = yyjson_obj_get(obj, "schema-ids");
		if (schema_ids_val) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(schema_ids_val, idx, max, val) {
				result.schema_ids.push_back(yyjson_get_sint(val));
			}
		}
		else {
			throw IOException("RemoveSchemasUpdate required property 'schema-ids' is missing");
		}

		return result;
	}

public:
	BaseUpdate base_update;
	string action;
	vector<int64_t> schema_ids;
};
} // namespace rest_api_objects
} // namespace duckdb