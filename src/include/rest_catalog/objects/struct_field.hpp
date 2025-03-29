#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/primitive_type_value.hpp"
#include "rest_catalog/objects/type.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class StructField {
public:
	static StructField FromJSON(yyjson_val *obj) {
		StructField result;

		auto doc_val = yyjson_obj_get(obj, "doc");
		if (doc_val) {
			result.doc = yyjson_get_str(doc_val);
		}

		auto id_val = yyjson_obj_get(obj, "id");
		if (id_val) {
			result.id = yyjson_get_sint(id_val);
		} else {
			throw IOException("StructField required property 'id' is missing");
		}

		auto initial_default_val = yyjson_obj_get(obj, "initial-default");
		if (initial_default_val) {
			result.initial_default = PrimitiveTypeValue::FromJSON(initial_default_val);
		}

		auto name_val = yyjson_obj_get(obj, "name");
		if (name_val) {
			result.name = yyjson_get_str(name_val);
		} else {
			throw IOException("StructField required property 'name' is missing");
		}

		auto required_val = yyjson_obj_get(obj, "required");
		if (required_val) {
			result.required = yyjson_get_bool(required_val);
		} else {
			throw IOException("StructField required property 'required' is missing");
		}

		auto type_val = yyjson_obj_get(obj, "type");
		if (type_val) {
			result.type = Type::FromJSON(type_val);
		} else {
			throw IOException("StructField required property 'type' is missing");
		}

		auto write_default_val = yyjson_obj_get(obj, "write-default");
		if (write_default_val) {
			result.write_default = PrimitiveTypeValue::FromJSON(write_default_val);
		}

		return result;
	}

public:
	string doc;
	int64_t id;
	PrimitiveTypeValue initial_default;
	string name;
	bool required;
	Type type;
	PrimitiveTypeValue write_default;
};
} // namespace rest_api_objects
} // namespace duckdb
