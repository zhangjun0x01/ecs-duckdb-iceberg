#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/plan_task.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class FetchScanTasksRequest {
public:
	static FetchScanTasksRequest FromJSON(yyjson_val *obj) {
		FetchScanTasksRequest result;

		auto plan_task_val = yyjson_obj_get(obj, "plan-task");
		if (plan_task_val) {
			result.plan_task = PlanTask::FromJSON(plan_task_val);
		}
		else {
			throw IOException("FetchScanTasksRequest required property 'plan-task' is missing");
		}

		return result;
	}

public:
	PlanTask plan_task;
};
} // namespace rest_api_objects
} // namespace duckdb