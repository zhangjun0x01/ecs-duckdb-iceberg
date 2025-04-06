
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/completed_planning_result.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class CompletedPlanningWithIDResult {
public:
	CompletedPlanningWithIDResult() {
	}

public:
	class Object6 {
	public:
		Object6() {
		}

	public:
		static Object6 FromJSON(yyjson_val *obj) {
			Object6 res;
			auto error = res.TryFromJSON(obj);
			if (!error.empty()) {
				throw InvalidInputException(error);
			}
			return res;
		}

	public:
		string TryFromJSON(yyjson_val *obj) {
			string error;

			auto plan_id_val = yyjson_obj_get(obj, "plan_id");
			if (plan_id_val) {
				plan_id = yyjson_get_str(plan_id_val);
			}

			return string();
		}

	public:
	public:
		string plan_id;
	};

public:
	static CompletedPlanningWithIDResult FromJSON(yyjson_val *obj) {
		CompletedPlanningWithIDResult res;
		auto error = res.TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return res;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;
		error = completed_planning_result.TryFromJSON(obj);
		if (!error.empty()) {
			return error;
		}
		error = object_6.TryFromJSON(obj);
		if (!error.empty()) {
			return error;
		}

		return string();
	}

public:
	CompletedPlanningResult completed_planning_result;
	Object6 object_6;

public:
};

} // namespace rest_api_objects
} // namespace duckdb
