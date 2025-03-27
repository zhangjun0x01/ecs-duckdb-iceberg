#pragma once

#include "yyjson.hpp"
#include <string>
#include <vector>
#include <unordered_map>
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class OAuthClientCredentialsRequest {
public:
	static OAuthClientCredentialsRequest FromJSON(yyjson_val *obj) {
		OAuthClientCredentialsRequest result;
		auto grant_type_val = yyjson_obj_get(obj, "grant_type");
		if (grant_type_val) {
			result.grant_type = yyjson_get_str(grant_type_val);
		}
		auto scope_val = yyjson_obj_get(obj, "scope");
		if (scope_val) {
			result.scope = yyjson_get_str(scope_val);
		}
		auto client_id_val = yyjson_obj_get(obj, "client_id");
		if (client_id_val) {
			result.client_id = yyjson_get_str(client_id_val);
		}
		auto client_secret_val = yyjson_obj_get(obj, "client_secret");
		if (client_secret_val) {
			result.client_secret = yyjson_get_str(client_secret_val);
		}
		return result;
	}
public:
	string grant_type;
	string scope;
	string client_id;
	string client_secret;
};

} // namespace rest_api_objects
} // namespace duckdb