
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
	CounterResult::CounterResult() {
	}

public:
	static CounterResult FromJSON(yyjson_val *obj) {
		auto error = TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return *this;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		auto unit_val = yyjson_obj_get(obj, "unit");
		if (!unit_val) {
		return "CounterResult required property 'unit' is missing");
		}
		result.unit = yyjson_get_str(unit_val);

		auto value_val = yyjson_obj_get(obj, "value");
		if (!value_val) {
		return "CounterResult required property 'value' is missing");
		}
		result.value = yyjson_get_sint(value_val);

		return string();
	}

public:
public:
};

} // namespace rest_api_objects
} // namespace duckdb
