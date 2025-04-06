
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/oauth_client_credentials_request.hpp"
#include "rest_catalog/objects/oauth_token_exchange_request.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class OAuthTokenRequest {
public:
	OAuthTokenRequest::OAuthTokenRequest() {
	}

public:
	static OAuthTokenRequest FromJSON(yyjson_val *obj) {
		auto error = TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return *this;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		error = oauth_client_credentials_request.TryFromJSON(obj);
		if (error.empty()) {
			has_oauth_client_credentials_request = true;
		}

		error = oauth_token_exchange_request.TryFromJSON(obj);
		if (error.empty()) {
			has_oauth_token_exchange_request = true;
		}

		if (!has_oauth_client_credentials_request && !has_oauth_token_exchange_request) {
			return "OAuthTokenRequest failed to parse, none of the anyOf candidates matched";
		}

		return string();
	}

public:
	OAuthTokenExchangeRequest oauth_token_exchange_request;
	OAuthClientCredentialsRequest oauth_client_credentials_request;

public:
	bool has_oauth_client_credentials_request = false;
	bool has_oauth_token_exchange_request = false;
};

} // namespace rest_api_objects
} // namespace duckdb
