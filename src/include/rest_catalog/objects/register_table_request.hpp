#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class RegisterTableRequest {
public:
	static RegisterTableRequest FromJSON(yyjson_val *obj) {
		RegisterTableRequest result;

		auto name_val = yyjson_obj_get(obj, "name");
		if (name_val) {
			result.name = yyjson_get_str(name_val);
		}
		else {
			throw IOException("RegisterTableRequest required property 'name' is missing");
		}

		auto metadata_location_val = yyjson_obj_get(obj, "metadata-location");
		if (metadata_location_val) {
			result.metadata_location = yyjson_get_str(metadata_location_val);
		}
		else {
			throw IOException("RegisterTableRequest required property 'metadata-location' is missing");
		}

		auto overwrite_val = yyjson_obj_get(obj, "overwrite");
		if (overwrite_val) {
			result.overwrite = yyjson_get_bool(overwrite_val);
		}

		return result;
	}

public:
	string name;
	string metadata_location;
	bool overwrite;
};
} // namespace rest_api_objects
} // namespace duckdb