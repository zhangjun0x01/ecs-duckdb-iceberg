
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class CounterResult {
public:
	CounterResult() {
	}

public:
	static CounterResult FromJSON(yyjson_val *obj) {
		CounterResult res;
		auto error = res.TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return res;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		auto unit_val = yyjson_obj_get(obj, "unit");
		if (!unit_val) {
			return "CounterResult required property 'unit' is missing";
		} else {
			unit = yyjson_get_str(unit_val);
		}

		auto value_val = yyjson_obj_get(obj, "value");
		if (!value_val) {
			return "CounterResult required property 'value' is missing";
		} else {
			value = yyjson_get_sint(value_val);
		}

		return string();
	}

public:
public:
	string unit;
	int64_t value;
};

} // namespace rest_api_objects
} // namespace duckdb
