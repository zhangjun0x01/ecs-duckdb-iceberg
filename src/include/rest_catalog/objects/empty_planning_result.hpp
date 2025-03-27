#pragma once

#include "yyjson.hpp"
#include <string>
#include <vector>
#include <unordered_map>
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/plan_status.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class EmptyPlanningResult {
public:
	static EmptyPlanningResult FromJSON(yyjson_val *obj) {
		EmptyPlanningResult result;
		auto status_val = yyjson_obj_get(obj, "status");
		if (status_val) {
			result.status = PlanStatus::FromJSON(status_val);
		}
		return result;
	}
public:
	PlanStatus status;
};

} // namespace rest_api_objects
} // namespace duckdb