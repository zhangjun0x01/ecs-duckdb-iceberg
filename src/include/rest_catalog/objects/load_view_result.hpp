#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/view_metadata.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class LoadViewResult {
public:
	static LoadViewResult FromJSON(yyjson_val *obj) {
		LoadViewResult result;

		auto metadata_location_val = yyjson_obj_get(obj, "metadata-location");
		if (metadata_location_val) {
			result.metadata_location = yyjson_get_str(metadata_location_val);
		}
		else {
			throw IOException("LoadViewResult required property 'metadata-location' is missing");
		}

		auto metadata_val = yyjson_obj_get(obj, "metadata");
		if (metadata_val) {
			result.metadata = ViewMetadata::FromJSON(metadata_val);
		}
		else {
			throw IOException("LoadViewResult required property 'metadata' is missing");
		}

		auto config_val = yyjson_obj_get(obj, "config");
		if (config_val) {
			result.config = parse_object_of_strings(config_val);
		}

		return result;
	}

public:
	string metadata_location;
	ViewMetadata metadata;
	ObjectOfStrings config;
};
} // namespace rest_api_objects
} // namespace duckdb