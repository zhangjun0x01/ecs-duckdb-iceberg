#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
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
		else {
			throw IOException("StorageCredential required property 'prefix' is missing");
		}

		auto config_val = yyjson_obj_get(obj, "config");
		if (config_val) {
			result.config = parse_object_of_strings(config_val);
		}
		else {
			throw IOException("StorageCredential required property 'config' is missing");
		}

		return result;
	}

public:
	string prefix;
	ObjectOfStrings config;
};
} // namespace rest_api_objects
} // namespace duckdb