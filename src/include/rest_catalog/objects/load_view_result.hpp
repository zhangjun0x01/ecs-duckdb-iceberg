
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/view_metadata.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class LoadViewResult {
public:
	LoadViewResult::LoadViewResult() {
	}

public:
	static LoadViewResult FromJSON(yyjson_val *obj) {
		auto error = TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return *this;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		auto metadata_location_val = yyjson_obj_get(obj, "metadata_location");
		if (!metadata_location_val) {
		return "LoadViewResult required property 'metadata_location' is missing");
		}
		metadata_location = yyjson_get_str(metadata_location_val);

		auto metadata_val = yyjson_obj_get(obj, "metadata");
		if (!metadata_val) {
		return "LoadViewResult required property 'metadata' is missing");
		}
		error = metadata.TryFromJSON(metadata_val);
		if (!error.empty()) {
			return error;
		}

		auto config_val = yyjson_obj_get(obj, "config");
		if (config_val) {
			config = parse_object_of_strings(config_val);
		}

		return string();
	}

public:
public:
	case_insensitive_map_t<string> config;
	ViewMetadata metadata;
	string metadata_location;
};

} // namespace rest_api_objects
} // namespace duckdb
