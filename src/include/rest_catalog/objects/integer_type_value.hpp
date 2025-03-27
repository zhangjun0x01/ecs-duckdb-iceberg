#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class IntegerTypeValue {
public:
	static IntegerTypeValue FromJSON(yyjson_val *obj) {
		IntegerTypeValue result;

		return result;
	}

public:
};
} // namespace rest_api_objects
} // namespace duckdb