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
	static BooleanTypeValue FromJSON(yyjson_val *obj) {
		BooleanTypeValue result;
		result.value = yyjson_get_bool(obj);
		return result;
	}

public:
	bool value;
};
} // namespace rest_api_objects
} // namespace duckdb
