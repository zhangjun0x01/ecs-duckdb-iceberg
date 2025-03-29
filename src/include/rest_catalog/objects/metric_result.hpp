#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class MetricResult {
public:
	static MetricResult FromJSON(yyjson_val *obj) {
		MetricResult result;
		if (yyjson_is_obj(obj)) {
			if (yyjson_obj_get(obj, "value") && yyjson_obj_get(obj, "unit")) {
				result.counter_result = CounterResult::FromJSON(obj);
				result.has_counter_result = true;
			}
			if (yyjson_obj_get(obj, "count") && yyjson_obj_get(obj, "total-duration") &&
			    yyjson_obj_get(obj, "time-unit")) {
				result.timer_result = TimerResult::FromJSON(obj);
				result.has_timer_result = true;
			}
			if (!(result.has_counter_result || result.has_timer_result)) {
				throw IOException("MetricResult failed to parse, none of the accepted schemas found");
			}
		} else {
			throw IOException("MetricResult must be an object");
		}
		return result;
	}

public:
	CounterResult counter_result;
	bool has_counter_result = false;
	TimerResult timer_result;
	bool has_timer_result = false;
};
} // namespace rest_api_objects
} // namespace duckdb
