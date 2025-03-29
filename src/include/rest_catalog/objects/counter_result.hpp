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
	static CounterResult FromJSON(yyjson_val *obj) {
		CounterResult result;

		auto unit_val = yyjson_obj_get(obj, "unit");
		if (unit_val) {
			result.unit = yyjson_get_str(unit_val);
		} else {
			throw IOException("CounterResult required property 'unit' is missing");
		}

		auto value_val = yyjson_obj_get(obj, "value");
		if (value_val) {
			result.value = yyjson_get_sint(value_val);
		} else {
			throw IOException("CounterResult required property 'value' is missing");
		}

		return result;
	}

public:
	string unit;
	int64_t value;
};
} // namespace rest_api_objects
} // namespace duckdb
