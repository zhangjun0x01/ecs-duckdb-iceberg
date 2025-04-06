
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class OAuthClientCredentialsRequest {
public:
	OAuthClientCredentialsRequest::OAuthClientCredentialsRequest() {
	}

public:
	static OAuthClientCredentialsRequest FromJSON(yyjson_val *obj) {
		auto error = TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return *this;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		auto grant_type_val = yyjson_obj_get(obj, "grant_type");
		if (!grant_type_val) {
		return "OAuthClientCredentialsRequest required property 'grant_type' is missing");
		}
		result.grant_type = yyjson_get_str(grant_type_val);

		auto client_id_val = yyjson_obj_get(obj, "client_id");
		if (!client_id_val) {
		return "OAuthClientCredentialsRequest required property 'client_id' is missing");
		}
		result.client_id = yyjson_get_str(client_id_val);

		auto client_secret_val = yyjson_obj_get(obj, "client_secret");
		if (!client_secret_val) {
		return "OAuthClientCredentialsRequest required property 'client_secret' is missing");
		}
		result.client_secret = yyjson_get_str(client_secret_val);

		auto scope_val = yyjson_obj_get(obj, "scope");
		if (scope_val) {
			result.scope = yyjson_get_str(scope_val);
		}
		return string();
	}

public:
public:
	string client_id;
	string client_secret;
	string grant_type;
	string scope;
};

} // namespace rest_api_objects
} // namespace duckdb
