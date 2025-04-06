
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/async_planning_result.hpp"
#include "rest_catalog/objects/completed_planning_with_idresult.hpp"
#include "rest_catalog/objects/empty_planning_result.hpp"
#include "rest_catalog/objects/failed_planning_result.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class PlanTableScanResult {
public:
	PlanTableScanResult::PlanTableScanResult() {
	}

public:
	static PlanTableScanResult FromJSON(yyjson_val *obj) {
		auto error = TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return *this;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;
		do {
			error = base_completed_planning_with_idresult.TryFromJSON(obj);
			if (error.empty()) {
				has_completed_planning_with_idresult = true;
				break;
			}
			error = base_failed_planning_result.TryFromJSON(obj);
			if (error.empty()) {
				has_failed_planning_result = true;
				break;
			}
			error = base_async_planning_result.TryFromJSON(obj);
			if (error.empty()) {
				has_async_planning_result = true;
				break;
			}
			error = base_empty_planning_result.TryFromJSON(obj);
			if (error.empty()) {
				has_empty_planning_result = true;
				break;
			}
			return "PlanTableScanResult failed to parse, none of the oneOf candidates matched";
		} while (false);

		return string();
	}

public:
	AsyncPlanningResult async_planning_result;
	EmptyPlanningResult empty_planning_result;
	CompletedPlanningWithIDResult completed_planning_with_idresult;
	FailedPlanningResult failed_planning_result;

public:
};

} // namespace rest_api_objects
} // namespace duckdb
