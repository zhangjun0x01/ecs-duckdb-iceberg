#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class Type {
public:
	static Type FromJSON(yyjson_val *obj) {
		Type result;
		if (yyjson_is_str(obj)) {
			result.value_string = yyjson_get_str(obj);
			result.has_string = true;
		}
		if (yyjson_is_obj(obj)) {
			auto type_val = yyjson_obj_get(obj, "type");
			if (type_val && strcmp(yyjson_get_str(type_val), "struct") == 0) {
				result.struct_type = StructType::FromJSON(obj);
				result.has_struct_type = true;
			}
			if (type_val && strcmp(yyjson_get_str(type_val), "list") == 0) {
				result.list_type = ListType::FromJSON(obj);
				result.has_list_type = true;
			}
			if (type_val && strcmp(yyjson_get_str(type_val), "map") == 0) {
				result.map_type = MapType::FromJSON(obj);
				result.has_map_type = true;
			}
		}
		return result;
	}

public:
	string value_string;
	bool has_string = false;
	StructType struct_type;
	bool has_struct_type = false;
	ListType list_type;
	bool has_list_type = false;
	MapType map_type;
	bool has_map_type = false;
};
} // namespace rest_api_objects
} // namespace duckdb
