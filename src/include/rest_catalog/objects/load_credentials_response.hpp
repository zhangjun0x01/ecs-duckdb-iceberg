#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/storage_credential.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class LoadCredentialsResponse {
public:
	static LoadCredentialsResponse FromJSON(yyjson_val *obj) {
		LoadCredentialsResponse result;

		auto storage_credentials_val = yyjson_obj_get(obj, "storage-credentials");
		if (storage_credentials_val) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(storage_credentials_val, idx, max, val) {
				result.storage_credentials.push_back(StorageCredential::FromJSON(val));
			}
		} else {
			throw IOException("LoadCredentialsResponse required property 'storage-credentials' is missing");
		}

		return result;
	}

public:
	vector<StorageCredential> storage_credentials;
};
} // namespace rest_api_objects
} // namespace duckdb
