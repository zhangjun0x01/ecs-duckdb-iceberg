
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class BooleanTypeValue {
public:
	BooleanTypeValue() {
	}

public:
	static BooleanTypeValue FromJSON(yyjson_val *obj) {
		BooleanTypeValue res;
		auto error = res.TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return res;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		value = yyjson_get_bool(obj);

		return string();
	}

public:
public:
	bool value;
};

} // namespace rest_api_objects
} // namespace duckdb
