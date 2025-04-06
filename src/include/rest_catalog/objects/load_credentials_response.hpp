
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
	LoadCredentialsResponse::LoadCredentialsResponse() {
	}

public:
	static LoadCredentialsResponse FromJSON(yyjson_val *obj) {
		auto error = TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return *this;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		auto storage_credentials_val = yyjson_obj_get(obj, "storage_credentials");
		if (!storage_credentials_val) {
		return "LoadCredentialsResponse required property 'storage_credentials' is missing");
		}
		size_t idx, max;
		yyjson_val *val;
		yyjson_arr_foreach(storage_credentials_val, idx, max, val) {
			result.storage_credentials.push_back(StorageCredential::FromJSON(val));
		}

		return string();
	}

public:
public:
	vector<StorageCredential> storage_credentials;
};

} // namespace rest_api_objects
} // namespace duckdb
