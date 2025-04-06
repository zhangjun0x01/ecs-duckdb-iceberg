
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/token_type.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class OAuthTokenExchangeRequest {
public:
	OAuthTokenExchangeRequest();

public:
	static OAuthTokenExchangeRequest FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	string grant_type;
	string subject_token;
	TokenType subject_token_type;
	string scope;
	TokenType requested_token_type;
	string actor_token;
	TokenType actor_token_type;
};

} // namespace rest_api_objects
} // namespace duckdb
