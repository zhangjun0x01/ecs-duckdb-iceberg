
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class LongTypeValue {
public:
	LongTypeValue() {
	}

public:
	static LongTypeValue FromJSON(yyjson_val *obj) {
		LongTypeValue res;
		auto error = res.TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return res;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		value = yyjson_get_sint(obj);

		return string();
	}

public:
public:
	int64_t value;
};

} // namespace rest_api_objects
} // namespace duckdb
