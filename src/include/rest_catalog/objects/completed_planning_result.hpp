
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/plan_status.hpp"
#include "rest_catalog/objects/scan_tasks.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class CompletedPlanningResult {
public:
	CompletedPlanningResult::CompletedPlanningResult() {
	}

public:
	class Object5 {
	public:
		Object5::Object5() {
		}

	public:
		static Object5 FromJSON(yyjson_val *obj) {
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
			return "Object5 required property 'status' is missing");
			}
			error = status.TryFromJSON(status_val);
			if (!error.empty()) {
				return error;
			}

			return string();
		}

	public:
	public:
		PlanStatus status;
	};

public:
	static CompletedPlanningResult FromJSON(yyjson_val *obj) {
		auto error = TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return *this;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		error = scan_tasks.TryFromJSON(obj);
		if (!error.empty()) {
			return error;
		}

		error = object_5.TryFromJSON(obj);
		if (!error.empty()) {
			return error;
		}

		return string();
	}

public:
	ScanTasks scan_tasks;
	Object5 object_5;

public:
};

} // namespace rest_api_objects
} // namespace duckdb
