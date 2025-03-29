#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/struct_type.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class Schema {
public:
	static Schema FromJSON(yyjson_val *obj) {
		Schema result;

		// Parse StructType fields
		result.struct_type = StructType::FromJSON(obj);

		auto identifier_field_ids_val = yyjson_obj_get(obj, "identifier-field-ids");
		if (identifier_field_ids_val) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(identifier_field_ids_val, idx, max, val) {
				result.identifier_field_ids.push_back(yyjson_get_sint(val));
			}
		}

		auto schema_id_val = yyjson_obj_get(obj, "schema-id");
		if (schema_id_val) {
			result.schema_id = yyjson_get_sint(schema_id_val);
		}

		return result;
	}

public:
	StructType struct_type;
	vector<int64_t> identifier_field_ids;
	int64_t schema_id;
};
} // namespace rest_api_objects
} // namespace duckdb
