#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class OAuthTokenRequest {
public:
	static OAuthTokenRequest FromJSON(yyjson_val *obj) {
		OAuthTokenRequest result;
		if (yyjson_is_obj(obj)) {
			if (yyjson_obj_get(obj, "grant_type") && yyjson_obj_get(obj, "client_id") &&
			    yyjson_obj_get(obj, "client_secret")) {
				result.oauth_client_credentials_request = OAuthClientCredentialsRequest::FromJSON(obj);
				result.has_oauth_client_credentials_request = true;
			}
			if (yyjson_obj_get(obj, "grant_type") && yyjson_obj_get(obj, "subject_token") &&
			    yyjson_obj_get(obj, "subject_token_type")) {
				result.oauth_token_exchange_request = OAuthTokenExchangeRequest::FromJSON(obj);
				result.has_oauth_token_exchange_request = true;
			}
			if (!(result.has_oauth_client_credentials_request || result.has_oauth_token_exchange_request)) {
				throw IOException("OAuthTokenRequest failed to parse, none of the accepted schemas found");
			}
		} else {
			throw IOException("OAuthTokenRequest must be an object");
		}
		return result;
	}

public:
	OAuthClientCredentialsRequest oauth_client_credentials_request;
	bool has_oauth_client_credentials_request = false;
	OAuthTokenExchangeRequest oauth_token_exchange_request;
	bool has_oauth_token_exchange_request = false;
};
} // namespace rest_api_objects
} // namespace duckdb
