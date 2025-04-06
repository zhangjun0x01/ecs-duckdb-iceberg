
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
	StructField() {
	}

public:
	static StructField FromJSON(yyjson_val *obj) {
		StructField res;
		auto error = res.TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return res;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;
		auto id_val = yyjson_obj_get(obj, "id");
		if (!id_val) {
			return "StructField required property 'id' is missing";
		} else {
			id = yyjson_get_sint(id_val);
		}
		auto name_val = yyjson_obj_get(obj, "name");
		if (!name_val) {
			return "StructField required property 'name' is missing";
		} else {
			name = yyjson_get_str(name_val);
		}
		auto type_val = yyjson_obj_get(obj, "type");
		if (!type_val) {
			return "StructField required property 'type' is missing";
		} else {
			type = make_uniq<Type>();
			error = type->TryFromJSON(type_val);
			if (!error.empty()) {
				return error;
			}
		}
		auto required_val = yyjson_obj_get(obj, "required");
		if (!required_val) {
			return "StructField required property 'required' is missing";
		} else {
			required = yyjson_get_bool(required_val);
		}
		auto doc_val = yyjson_obj_get(obj, "doc");
		if (doc_val) {
			doc = yyjson_get_str(doc_val);
		}
		auto initial_default_val = yyjson_obj_get(obj, "initial_default");
		if (initial_default_val) {
			error = initial_default.TryFromJSON(initial_default_val);
			if (!error.empty()) {
				return error;
			}
		}
		auto write_default_val = yyjson_obj_get(obj, "write_default");
		if (write_default_val) {
			error = write_default.TryFromJSON(write_default_val);
			if (!error.empty()) {
				return error;
			}
		}
		return string();
	}

public:
	int64_t id;
	string name;
	unique_ptr<Type> type;
	bool required;
	string doc;
	PrimitiveTypeValue initial_default;
	PrimitiveTypeValue write_default;
};

} // namespace rest_api_objects
} // namespace duckdb
