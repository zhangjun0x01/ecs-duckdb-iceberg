#pragma once

#include "yyjson.hpp"
#include <string>
#include <vector>
#include <unordered_map>
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/struct_field.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class StructType {
public:
	static StructType FromJSON(yyjson_val *obj) {
		StructType result;

		auto type_val = yyjson_obj_get(obj, "type");
		if (type_val) {
			result.type = yyjson_get_str(type_val);
		}
		else {
			throw IOException("StructType required property 'type' is missing");
		}

		auto fields_val = yyjson_obj_get(obj, "fields");
		if (fields_val) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(fields_val, idx, max, val) {
				result.fields.push_back(StructField::FromJSON(val));
			}
		}
		else {
			throw IOException("StructType required property 'fields' is missing");
		}

		return result;
	}

public:
	string type;
	vector<StructField> fields;
};
} // namespace rest_api_objects
} // namespace duckdb