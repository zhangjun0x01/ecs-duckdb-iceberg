#pragma once

#include "yyjson.hpp"
#include <string>
#include <vector>
#include <unordered_map>
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class StorageCredential {
public:
	static StorageCredential FromJSON(yyjson_val *obj) {
		StorageCredential result;
		auto prefix_val = yyjson_obj_get(obj, "prefix");
		if (prefix_val) {
			result.prefix = yyjson_get_str(prefix_val);
		}
		auto config_val = yyjson_obj_get(obj, "config");
		if (config_val) {
			result.config = parse_object_of_strings(config_val);
		}
		return result;
	}
public:
	string prefix;
	ObjectOfStrings config;
};

} // namespace rest_api_objects
} // namespace duckdb