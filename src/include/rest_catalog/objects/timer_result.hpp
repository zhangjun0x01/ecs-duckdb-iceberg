
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class TimerResult {
public:
	TimerResult::TimerResult() {
	}

public:
	static TimerResult FromJSON(yyjson_val *obj) {
		auto error = TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return *this;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		auto time_unit_val = yyjson_obj_get(obj, "time_unit");
		if (!time_unit_val) {
		return "TimerResult required property 'time_unit' is missing");
		}
		result.time_unit = yyjson_get_str(time_unit_val);

		auto count_val = yyjson_obj_get(obj, "count");
		if (!count_val) {
		return "TimerResult required property 'count' is missing");
		}
		result.count = yyjson_get_sint(count_val);

		auto total_duration_val = yyjson_obj_get(obj, "total_duration");
		if (!total_duration_val) {
		return "TimerResult required property 'total_duration' is missing");
		}
		result.total_duration = yyjson_get_sint(total_duration_val);

		return string();
	}

public:
public:
	string time_unit;
	int64_t count;
	int64_t total_duration;
};

} // namespace rest_api_objects
} // namespace duckdb
