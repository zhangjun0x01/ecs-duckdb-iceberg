#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class OAuthError {
public:
	static OAuthError FromJSON(yyjson_val *obj) {
		OAuthError result;

		auto error_val = yyjson_obj_get(obj, "error");
		if (error_val) {
			result.error = yyjson_get_str(error_val);
		}
		else {
			throw IOException("OAuthError required property 'error' is missing");
		}

		auto error_description_val = yyjson_obj_get(obj, "error_description");
		if (error_description_val) {
			result.error_description = yyjson_get_str(error_description_val);
		}

		auto error_uri_val = yyjson_obj_get(obj, "error_uri");
		if (error_uri_val) {
			result.error_uri = yyjson_get_str(error_uri_val);
		}

		return result;
	}

public:
	string error;
	string error_description;
	string error_uri;
};
} // namespace rest_api_objects
} // namespace duckdb