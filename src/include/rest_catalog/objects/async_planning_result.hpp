#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/plan_status.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class AsyncPlanningResult {
public:
	static AsyncPlanningResult FromJSON(yyjson_val *obj) {
		AsyncPlanningResult result;

		auto plan_id_val = yyjson_obj_get(obj, "plan-id");
		if (plan_id_val) {
			result.plan_id = yyjson_get_str(plan_id_val);
		}

		auto status_val = yyjson_obj_get(obj, "status");
		if (status_val) {
			result.status = PlanStatus::FromJSON(status_val);
		}
		else {
			throw IOException("AsyncPlanningResult required property 'status' is missing");
		}

		return result;
	}

public:
	string plan_id;
	PlanStatus status;
};
} // namespace rest_api_objects
} // namespace duckdb