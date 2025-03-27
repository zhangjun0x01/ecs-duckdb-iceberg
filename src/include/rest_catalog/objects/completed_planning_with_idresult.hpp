#pragma once

#include "yyjson.hpp"
#include <string>
#include <vector>
#include <unordered_map>
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/completed_planning_result.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class CompletedPlanningWithIDResult {
public:
	static CompletedPlanningWithIDResult FromJSON(yyjson_val *obj) {
		CompletedPlanningWithIDResult result;

		// Parse CompletedPlanningResult fields
		result.completed_planning_result = CompletedPlanningResult::FromJSON(obj);

		auto plan_id_val = yyjson_obj_get(obj, "plan-id");
		if (plan_id_val) {
			result.plan_id = yyjson_get_str(plan_id_val);
		}

		return result;
	}

public:
	CompletedPlanningResult completed_planning_result;
	string plan_id;
};
} // namespace rest_api_objects
} // namespace duckdb