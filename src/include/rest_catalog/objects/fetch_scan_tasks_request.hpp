
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/plan_task.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class FetchScanTasksRequest {
public:
	FetchScanTasksRequest::FetchScanTasksRequest() {
	}

public:
	static FetchScanTasksRequest FromJSON(yyjson_val *obj) {
		auto error = TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return *this;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		auto plan_task_val = yyjson_obj_get(obj, "plan_task");
		if (!plan_task_val) {
		return "FetchScanTasksRequest required property 'plan_task' is missing");
		}
		error = plan_task.TryFromJSON(plan_task_val);
		if (!error.empty()) {
			return error;
		}

		return string();
	}

public:
public:
	PlanTask plan_task;
};

} // namespace rest_api_objects
} // namespace duckdb
