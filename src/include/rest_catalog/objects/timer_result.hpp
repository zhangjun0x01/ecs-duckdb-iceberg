#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class TimerResult {
public:
	static TimerResult FromJSON(yyjson_val *obj) {
		TimerResult result;

		auto time_unit_val = yyjson_obj_get(obj, "time-unit");
		if (time_unit_val) {
			result.time_unit = yyjson_get_str(time_unit_val);
		}
		else {
			throw IOException("TimerResult required property 'time-unit' is missing");
		}

		auto count_val = yyjson_obj_get(obj, "count");
		if (count_val) {
			result.count = yyjson_get_sint(count_val);
		}
		else {
			throw IOException("TimerResult required property 'count' is missing");
		}

		auto total_duration_val = yyjson_obj_get(obj, "total-duration");
		if (total_duration_val) {
			result.total_duration = yyjson_get_sint(total_duration_val);
		}
		else {
			throw IOException("TimerResult required property 'total-duration' is missing");
		}

		return result;
	}

public:
	string time_unit;
	int64_t count;
	int64_t total_duration;
};
} // namespace rest_api_objects
} // namespace duckdb