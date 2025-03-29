#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class FetchPlanningResult {
public:
	static FetchPlanningResult FromJSON(yyjson_val *obj) {
		FetchPlanningResult result;
		auto discriminator_val = yyjson_obj_get(obj, "status");
		if (discriminator_val && strcmp(yyjson_get_str(discriminator_val), "cancelled") == 0) {
			result.empty_planning_result = EmptyPlanningResult::FromJSON(obj);
			result.has_empty_planning_result = true;
		} else if (discriminator_val && strcmp(yyjson_get_str(discriminator_val), "completed") == 0) {
			result.completed_planning_result = CompletedPlanningResult::FromJSON(obj);
			result.has_completed_planning_result = true;
		} else if (discriminator_val && strcmp(yyjson_get_str(discriminator_val), "failed") == 0) {
			result.failed_planning_result = FailedPlanningResult::FromJSON(obj);
			result.has_failed_planning_result = true;
		} else if (discriminator_val && strcmp(yyjson_get_str(discriminator_val), "submitted") == 0) {
			result.empty_planning_result = EmptyPlanningResult::FromJSON(obj);
			result.has_empty_planning_result = true;
		} else {
			throw IOException("FetchPlanningResult failed to parse, none of the accepted schemas found");
		}
		return result;
	}

public:
	CompletedPlanningResult completed_planning_result;
	bool has_completed_planning_result = false;
	FailedPlanningResult failed_planning_result;
	bool has_failed_planning_result = false;
	EmptyPlanningResult empty_planning_result;
	bool has_empty_planning_result = false;
};
} // namespace rest_api_objects
} // namespace duckdb
