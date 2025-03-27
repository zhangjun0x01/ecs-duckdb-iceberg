#pragma once

#include "yyjson.hpp"
#include <string>
#include <vector>
#include <unordered_map>
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/struct_type.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class Schema {
public:
	static Schema FromJSON(yyjson_val *obj) {
		Schema result;
		auto schema_id_val = yyjson_obj_get(obj, "schema-id");
		if (schema_id_val) {
			result.schema_id = yyjson_get_sint(schema_id_val);
		}
		auto identifier_field_ids_val = yyjson_obj_get(obj, "identifier-field-ids");
		if (identifier_field_ids_val) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(identifier_field_ids_val, idx, max, val) {
				result.identifier_field_ids.push_back(yyjson_get_sint(val));
			}
		}
		return result;
	}
public:
	int64_t schema_id;
	vector<int64_t> identifier_field_ids;
};

} // namespace rest_api_objects
} // namespace duckdb