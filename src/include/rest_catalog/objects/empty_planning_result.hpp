
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

class EmptyPlanningResult {
public:
	EmptyPlanningResult::EmptyPlanningResult() {
	}

public:
	static EmptyPlanningResult FromJSON(yyjson_val *obj) {
		auto error = TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return *this;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		auto status_val = yyjson_obj_get(obj, "status");
		if (!status_val) {
		return "EmptyPlanningResult required property 'status' is missing");
		}
		result.status = PlanStatus::FromJSON(status_val);

		return string();
	}

public:
public:
};

} // namespace rest_api_objects
} // namespace duckdb
