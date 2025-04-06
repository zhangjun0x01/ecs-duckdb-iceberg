
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
	StructType() {
	}

public:
	static StructType FromJSON(yyjson_val *obj) {
		StructType res;
		auto error = res.TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return res;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;
		auto type_val = yyjson_obj_get(obj, "type");
		if (!type_val) {
			return "StructType required property 'type' is missing";
		} else {
			type = yyjson_get_str(type_val);
		}
		auto fields_val = yyjson_obj_get(obj, "fields");
		if (!fields_val) {
			return "StructType required property 'fields' is missing";
		} else {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(fields_val, idx, max, val) {
				auto tmp_p = make_uniq<StructField>();
				auto &tmp = *tmp_p;
				error = tmp.TryFromJSON(val);
				if (!error.empty()) {
					return error;
				}
				fields.push_back(std::move(tmp_p));
			}
		}
		return string();
	}

public:
	string type;
	vector<unique_ptr<StructField>> fields;
};

} // namespace rest_api_objects
} // namespace duckdb
