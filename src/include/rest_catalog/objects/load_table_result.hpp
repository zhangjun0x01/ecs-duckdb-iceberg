
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/storage_credential.hpp"
#include "rest_catalog/objects/table_metadata.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class LoadTableResult {
public:
	LoadTableResult::LoadTableResult() {
	}

public:
	static LoadTableResult FromJSON(yyjson_val *obj) {
		auto error = TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return *this;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		auto metadata_val = yyjson_obj_get(obj, "metadata");
		if (!metadata_val) {
		return "LoadTableResult required property 'metadata' is missing");
		}
		result.metadata = TableMetadata::FromJSON(metadata_val);

		auto metadata_location_val = yyjson_obj_get(obj, "metadata_location");
		if (metadata_location_val) {
			result.metadata_location = yyjson_get_str(metadata_location_val);
		}

		auto config_val = yyjson_obj_get(obj, "config");
		if (config_val) {
			result.config = parse_object_of_strings(config_val);
		}

		auto storage_credentials_val = yyjson_obj_get(obj, "storage_credentials");
		if (storage_credentials_val) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(storage_credentials_val, idx, max, val) {
				result.storage_credentials.push_back(StorageCredential::FromJSON(val));
			}
		}
		return string();
	}

public:
public:
	string metadata_location;
	TableMetadata metadata;
	yyjson_val *config;
	vector<StorageCredential> storage_credentials;
};

} // namespace rest_api_objects
} // namespace duckdb
