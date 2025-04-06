
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/iceberg_error_response.hpp"
#include "rest_catalog/objects/plan_status.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class FailedPlanningResult {
public:
	FailedPlanningResult::FailedPlanningResult() {
	}

public:
	class Object7 {
	public:
		Object7::Object7() {
		}

	public:
		static Object7 FromJSON(yyjson_val *obj) {
			auto error = TryFromJSON(obj);
			if (!error.empty()) {
				throw InvalidInputException(error);
			}
			return *this;
		}

	public:
		string TryFromJSON(yyjson_val *obj) {
			string error;

			auto status_val = yyjson_obj_get(obj, "status");
			if (!status_val) {
			return "Object7 required property 'status' is missing");
			}
			error = status.TryFromJSON(status_val);
			if (!error.empty()) {
				return error;
			}

			return string();
		}

	public:
	public:
		PlanStatus status;
	};

public:
	static FailedPlanningResult FromJSON(yyjson_val *obj) {
		auto error = TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return *this;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		error = iceberg_error_response.TryFromJSON(obj);
		if (!error.empty()) {
			return error;
		}

		error = object_7.TryFromJSON(obj);
		if (!error.empty()) {
			return error;
		}

		return string();
	}

public:
	Object7 object_7;
	IcebergErrorResponse iceberg_error_response;

public:
};

} // namespace rest_api_objects
} // namespace duckdb
