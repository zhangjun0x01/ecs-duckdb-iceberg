
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
	IcebergErrorResponse::IcebergErrorResponse() {
	}

public:
	static IcebergErrorResponse FromJSON(yyjson_val *obj) {
		auto error = TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return *this;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		auto error_val = yyjson_obj_get(obj, "error");
		if (!error_val) {
		return "IcebergErrorResponse required property 'error' is missing");
		}
		result.error = ErrorModel::FromJSON(error_val);

		return string();
	}

public:
public:
};

} // namespace rest_api_objects
} // namespace duckdb
