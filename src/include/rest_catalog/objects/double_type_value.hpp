
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class DoubleTypeValue {
public:
	DoubleTypeValue::DoubleTypeValue() {
	}

public:
	static DoubleTypeValue FromJSON(yyjson_val *obj) {
		auto error = TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return *this;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		result.value = yyjson_get_real(obj);

		return string();
	}

public:
public:
	double value;
};

} // namespace rest_api_objects
} // namespace duckdb
