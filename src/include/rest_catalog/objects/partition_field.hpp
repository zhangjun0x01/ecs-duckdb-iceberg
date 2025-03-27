#pragma once

#include "yyjson.hpp"
#include <string>
#include <vector>
#include <unordered_map>
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/transform.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class PartitionField {
public:
	static PartitionField FromJSON(yyjson_val *obj) {
		PartitionField result;
		auto field_id_val = yyjson_obj_get(obj, "field-id");
		if (field_id_val) {
			result.field_id = yyjson_get_sint(field_id_val);
		}
		auto source_id_val = yyjson_obj_get(obj, "source-id");
		if (source_id_val) {
			result.source_id = yyjson_get_sint(source_id_val);
		}
		auto name_val = yyjson_obj_get(obj, "name");
		if (name_val) {
			result.name = yyjson_get_str(name_val);
		}
		auto transform_val = yyjson_obj_get(obj, "transform");
		if (transform_val) {
			result.transform = Transform::FromJSON(transform_val);
		}
		return result;
	}
public:
	int64_t field_id;
	int64_t source_id;
	string name;
	Transform transform;
};

} // namespace rest_api_objects
} // namespace duckdb