
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
	CompletedPlanningResult() {
	}

public:
	class Object5 {
	public:
		Object5() {
		}

	public:
		static Object5 FromJSON(yyjson_val *obj) {
			Object5 res;
			auto error = res.TryFromJSON(obj);
			if (!error.empty()) {
				throw InvalidInputException(error);
			}
			return res;
		}

	public:
		string TryFromJSON(yyjson_val *obj) {
			string error;

			auto status_val = yyjson_obj_get(obj, "status");
			if (!status_val) {
				return "Object5 required property 'status' is missing";
			} else {
				error = status.TryFromJSON(status_val);
				if (!error.empty()) {
					return error;
				}
			}

			return string();
		}

	public:
	public:
		PlanStatus status;
	};

public:
	static CompletedPlanningResult FromJSON(yyjson_val *obj) {
		CompletedPlanningResult res;
		auto error = res.TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return res;
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
	Object5 object_5;
	ScanTasks scan_tasks;

public:
};

} // namespace rest_api_objects
} // namespace duckdb
