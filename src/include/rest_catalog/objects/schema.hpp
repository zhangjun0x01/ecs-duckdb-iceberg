
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
	Schema() {
	}
	class Object1 {
	public:
		Object1() {
		}

	public:
		static Object1 FromJSON(yyjson_val *obj) {
			Object1 res;
			auto error = res.TryFromJSON(obj);
			if (!error.empty()) {
				throw InvalidInputException(error);
			}
			return res;
		}

	public:
		string TryFromJSON(yyjson_val *obj) {
			string error;
			auto schema_id_val = yyjson_obj_get(obj, "schema_id");
			if (schema_id_val) {
				schema_id = yyjson_get_sint(schema_id_val);
			}
			auto identifier_field_ids_val = yyjson_obj_get(obj, "identifier_field_ids");
			if (identifier_field_ids_val) {
				size_t idx, max;
				yyjson_val *val;
				yyjson_arr_foreach(identifier_field_ids_val, idx, max, val) {
					auto tmp = yyjson_get_sint(val);
					identifier_field_ids.push_back(tmp);
				}
			}
			return string();
		}

	public:
		int64_t schema_id;
		vector<int64_t> identifier_field_ids;
	};

public:
	static Schema FromJSON(yyjson_val *obj) {
		Schema res;
		auto error = res.TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return res;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;
		error = struct_type.TryFromJSON(obj);
		if (!error.empty()) {
			return error;
		}
		error = object_1.TryFromJSON(obj);
		if (!error.empty()) {
			return error;
		}
		return string();
	}

public:
	StructType struct_type;
	Object1 object_1;
};

} // namespace rest_api_objects
} // namespace duckdb
