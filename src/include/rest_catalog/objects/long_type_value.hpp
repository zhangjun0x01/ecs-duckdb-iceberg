#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class LongTypeValue {
public:
	static LongTypeValue FromJSON(yyjson_val *obj) {
		LongTypeValue result;

		return result;
	}

public:
};
} // namespace rest_api_objects
} // namespace duckdb