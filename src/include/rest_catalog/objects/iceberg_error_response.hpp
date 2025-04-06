
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/error_model.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class IcebergErrorResponse {
public:
	IcebergErrorResponse() {
	}

public:
	static IcebergErrorResponse FromJSON(yyjson_val *obj) {
		IcebergErrorResponse res;
		auto error = res.TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return res;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		auto _error_val = yyjson_obj_get(obj, "_error");
		if (!_error_val) {
			return "IcebergErrorResponse required property '_error' is missing";
		} else {
			error = _error.TryFromJSON(_error_val);
			if (!error.empty()) {
				return error;
			}
		}

		return string();
	}

public:
public:
	ErrorModel _error;
};

} // namespace rest_api_objects
} // namespace duckdb
