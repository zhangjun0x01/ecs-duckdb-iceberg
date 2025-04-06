
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/counter_result.hpp"
#include "rest_catalog/objects/timer_result.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class MetricResult {
public:
	MetricResult() {
	}

public:
	static MetricResult FromJSON(yyjson_val *obj) {
		MetricResult res;
		auto error = res.TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return res;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;
		error = counter_result.TryFromJSON(obj);
		if (error.empty()) {
			has_counter_result = true;
		}
		error = timer_result.TryFromJSON(obj);
		if (error.empty()) {
			has_timer_result = true;
		}

		if (!has_counter_result && !has_timer_result) {
			return "MetricResult failed to parse, none of the anyOf candidates matched";
		}

		return string();
	}

public:
	CounterResult counter_result;
	TimerResult timer_result;

public:
	bool has_counter_result = false;
	bool has_timer_result = false;
};

} // namespace rest_api_objects
} // namespace duckdb
