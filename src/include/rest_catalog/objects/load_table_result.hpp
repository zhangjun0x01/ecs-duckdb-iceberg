#pragma once

#include "yyjson.hpp"
#include <string>
#include <vector>
#include <unordered_map>
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/storage_credential.hpp"
#include "rest_catalog/objects/table_metadata.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class LoadTableResult {
public:
	static LoadTableResult FromJSON(yyjson_val *obj) {
		LoadTableResult result;

		auto metadata_location_val = yyjson_obj_get(obj, "metadata-location");
		if (metadata_location_val) {
			result.metadata_location = yyjson_get_str(metadata_location_val);
		}

		auto metadata_val = yyjson_obj_get(obj, "metadata");
		if (metadata_val) {
			result.metadata = TableMetadata::FromJSON(metadata_val);
		}
		else {
			throw IOException("LoadTableResult required property 'metadata' is missing");
		}

		auto config_val = yyjson_obj_get(obj, "config");
		if (config_val) {
			result.config = parse_object_of_strings(config_val);
		}

		auto storage_credentials_val = yyjson_obj_get(obj, "storage-credentials");
		if (storage_credentials_val) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(storage_credentials_val, idx, max, val) {
				result.storage_credentials.push_back(StorageCredential::FromJSON(val));
			}
		}

		return result;
	}

public:
	string metadata_location;
	TableMetadata metadata;
	ObjectOfStrings config;
	vector<StorageCredential> storage_credentials;
};
} // namespace rest_api_objects
} // namespace duckdb