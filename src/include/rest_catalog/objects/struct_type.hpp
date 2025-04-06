
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/struct_field.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class StructType {
public:
	StructType::StructType() {
	}

public:
	static StructType FromJSON(yyjson_val *obj) {
		auto error = TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return *this;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		auto type_val = yyjson_obj_get(obj, "type");
		if (!type_val) {
		return "StructType required property 'type' is missing");
		}
		result.type = yyjson_get_str(type_val);

		auto fields_val = yyjson_obj_get(obj, "fields");
		if (!fields_val) {
		return "StructType required property 'fields' is missing");
		}
		size_t idx, max;
		yyjson_val *val;
		yyjson_arr_foreach(fields_val, idx, max, val) {
			result.fields.push_back(StructField::FromJSON(val));
		}

		return string();
	}

public:
public:
	vector<StructField> fields;
	string type;
};

} // namespace rest_api_objects
} // namespace duckdb
