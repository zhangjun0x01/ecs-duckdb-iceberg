#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class PlanTableScanResult {
public:
	static PlanTableScanResult FromJSON(yyjson_val *obj) {
		PlanTableScanResult result;
		auto discriminator_val = yyjson_obj_get(obj, "status");
		if (discriminator_val && strcmp(yyjson_get_str(discriminator_val), "cancelled") == 0) {
			result.empty_planning_result = EmptyPlanningResult::FromJSON(obj);
			result.has_empty_planning_result = true;
		} else if (discriminator_val && strcmp(yyjson_get_str(discriminator_val), "completed") == 0) {
			result.completed_planning_with_idresult = CompletedPlanningWithIDResult::FromJSON(obj);
			result.has_completed_planning_with_idresult = true;
		} else if (discriminator_val && strcmp(yyjson_get_str(discriminator_val), "failed") == 0) {
			result.failed_planning_result = FailedPlanningResult::FromJSON(obj);
			result.has_failed_planning_result = true;
		} else if (discriminator_val && strcmp(yyjson_get_str(discriminator_val), "submitted") == 0) {
			result.async_planning_result = AsyncPlanningResult::FromJSON(obj);
			result.has_async_planning_result = true;
		} else {
			throw IOException("PlanTableScanResult failed to parse, none of the accepted schemas found");
		}
		return result;
	}

public:
	CompletedPlanningWithIDResult completed_planning_with_idresult;
	bool has_completed_planning_with_idresult = false;
	FailedPlanningResult failed_planning_result;
	bool has_failed_planning_result = false;
	AsyncPlanningResult async_planning_result;
	bool has_async_planning_result = false;
	EmptyPlanningResult empty_planning_result;
	bool has_empty_planning_result = false;
};
} // namespace rest_api_objects
} // namespace duckdb
