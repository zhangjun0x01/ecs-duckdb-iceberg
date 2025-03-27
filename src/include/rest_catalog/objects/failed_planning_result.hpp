#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/plan_status.hpp"
#include "rest_catalog/objects/iceberg_error_response.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class FailedPlanningResult {
public:
	static FailedPlanningResult FromJSON(yyjson_val *obj) {
		FailedPlanningResult result;

		// Parse IcebergErrorResponse fields
		result.iceberg_error_response = IcebergErrorResponse::FromJSON(obj);

		auto status_val = yyjson_obj_get(obj, "status");
		if (status_val) {
			result.status = PlanStatus::FromJSON(status_val);
		}
		else {
			throw IOException("FailedPlanningResult required property 'status' is missing");
		}

		return result;
	}

public:
	IcebergErrorResponse iceberg_error_response;
	PlanStatus status;
};
} // namespace rest_api_objects
} // namespace duckdb