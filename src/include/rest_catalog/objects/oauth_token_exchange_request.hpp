#pragma once

#include "yyjson.hpp"
#include <string>
#include <vector>
#include <unordered_map>
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/token_type.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class OAuthTokenExchangeRequest {
public:
	static OAuthTokenExchangeRequest FromJSON(yyjson_val *obj) {
		OAuthTokenExchangeRequest result;
		auto grant_type_val = yyjson_obj_get(obj, "grant_type");
		if (grant_type_val) {
			result.grant_type = yyjson_get_str(grant_type_val);
		}
		auto scope_val = yyjson_obj_get(obj, "scope");
		if (scope_val) {
			result.scope = yyjson_get_str(scope_val);
		}
		auto requested_token_type_val = yyjson_obj_get(obj, "requested_token_type");
		if (requested_token_type_val) {
			result.requested_token_type = TokenType::FromJSON(requested_token_type_val);
		}
		auto subject_token_val = yyjson_obj_get(obj, "subject_token");
		if (subject_token_val) {
			result.subject_token = yyjson_get_str(subject_token_val);
		}
		auto subject_token_type_val = yyjson_obj_get(obj, "subject_token_type");
		if (subject_token_type_val) {
			result.subject_token_type = TokenType::FromJSON(subject_token_type_val);
		}
		auto actor_token_val = yyjson_obj_get(obj, "actor_token");
		if (actor_token_val) {
			result.actor_token = yyjson_get_str(actor_token_val);
		}
		auto actor_token_type_val = yyjson_obj_get(obj, "actor_token_type");
		if (actor_token_type_val) {
			result.actor_token_type = TokenType::FromJSON(actor_token_type_val);
		}
		return result;
	}
public:
	string grant_type;
	string scope;
	TokenType requested_token_type;
	string subject_token;
	TokenType subject_token_type;
	string actor_token;
	TokenType actor_token_type;
};

} // namespace rest_api_objects
} // namespace duckdb