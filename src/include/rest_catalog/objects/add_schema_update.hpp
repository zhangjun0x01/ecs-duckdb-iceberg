#pragma once

#include "yyjson.hpp"
#include <string>
#include <vector>
#include <unordered_map>
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/base_update.hpp"
#include "rest_catalog/objects/schema.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class AddSchemaUpdate {
public:
	static AddSchemaUpdate FromJSON(yyjson_val *obj) {
		AddSchemaUpdate result;
		auto action_val = yyjson_obj_get(obj, "action");
		if (action_val) {
			result.action = yyjson_get_str(action_val);
		}
		auto schema_val = yyjson_obj_get(obj, "schema");
		if (schema_val) {
			result.schema = Schema::FromJSON(schema_val);
		}
		auto last_column_id_val = yyjson_obj_get(obj, "last-column-id");
		if (last_column_id_val) {
			result.last_column_id = yyjson_get_sint(last_column_id_val);
		}
		return result;
	}
public:
	string action;
	Schema schema;
	int64_t last_column_id;
};

} // namespace rest_api_objects
} // namespace duckdb