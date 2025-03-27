#pragma once

#include "yyjson.hpp"
#include <string>
#include <vector>
#include <unordered_map>
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/scan_tasks.hpp"
#include "rest_catalog/objects/plan_status.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class CompletedPlanningResult {
public:
	static CompletedPlanningResult FromJSON(yyjson_val *obj) {
		CompletedPlanningResult result;

		// Parse ScanTasks fields
		result.scan_tasks = ScanTasks::FromJSON(obj);

		auto status_val = yyjson_obj_get(obj, "status");
		if (status_val) {
			result.status = PlanStatus::FromJSON(status_val);
		}
		else {
			throw IOException("CompletedPlanningResult required property 'status' is missing");
		}

		return result;
	}

public:
	ScanTasks scan_tasks;
	PlanStatus status;
};
} // namespace rest_api_objects
} // namespace duckdb