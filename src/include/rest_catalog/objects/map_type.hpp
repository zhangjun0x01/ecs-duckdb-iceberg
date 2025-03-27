#pragma once

#include "yyjson.hpp"
#include <string>
#include <vector>
#include <unordered_map>
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/type.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class MapType {
public:
	static MapType FromJSON(yyjson_val *obj) {
		MapType result;
		auto type_val = yyjson_obj_get(obj, "type");
		if (type_val) {
			result.type = yyjson_get_str(type_val);
		}
		auto key_id_val = yyjson_obj_get(obj, "key-id");
		if (key_id_val) {
			result.key_id = yyjson_get_sint(key_id_val);
		}
		auto key_val = yyjson_obj_get(obj, "key");
		if (key_val) {
			result.key = Type::FromJSON(key_val);
		}
		auto value_id_val = yyjson_obj_get(obj, "value-id");
		if (value_id_val) {
			result.value_id = yyjson_get_sint(value_id_val);
		}
		auto value_val = yyjson_obj_get(obj, "value");
		if (value_val) {
			result.value = Type::FromJSON(value_val);
		}
		auto value_required_val = yyjson_obj_get(obj, "value-required");
		if (value_required_val) {
			result.value_required = yyjson_get_bool(value_required_val);
		}
		return result;
	}
public:
	string type;
	int64_t key_id;
	Type key;
	int64_t value_id;
	Type value;
	bool value_required;
};

} // namespace rest_api_objects
} // namespace duckdb