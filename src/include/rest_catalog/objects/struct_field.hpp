#pragma once

#include "yyjson.hpp"
#include <string>
#include <vector>
#include <unordered_map>
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/type.hpp"
#include "rest_catalog/objects/primitive_type_value.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class StructField {
public:
	static StructField FromJSON(yyjson_val *obj) {
		StructField result;
		auto id_val = yyjson_obj_get(obj, "id");
		if (id_val) {
			result.id = yyjson_get_sint(id_val);
		}
		auto name_val = yyjson_obj_get(obj, "name");
		if (name_val) {
			result.name = yyjson_get_str(name_val);
		}
		auto type_val = yyjson_obj_get(obj, "type");
		if (type_val) {
			result.type = Type::FromJSON(type_val);
		}
		auto required_val = yyjson_obj_get(obj, "required");
		if (required_val) {
			result.required = yyjson_get_bool(required_val);
		}
		auto doc_val = yyjson_obj_get(obj, "doc");
		if (doc_val) {
			result.doc = yyjson_get_str(doc_val);
		}
		auto initial_default_val = yyjson_obj_get(obj, "initial-default");
		if (initial_default_val) {
			result.initial_default = PrimitiveTypeValue::FromJSON(initial_default_val);
		}
		auto write_default_val = yyjson_obj_get(obj, "write-default");
		if (write_default_val) {
			result.write_default = PrimitiveTypeValue::FromJSON(write_default_val);
		}
		return result;
	}
public:
	int64_t id;
	string name;
	Type type;
	bool required;
	string doc;
	PrimitiveTypeValue initial_default;
	PrimitiveTypeValue write_default;
};

} // namespace rest_api_objects
} // namespace duckdb