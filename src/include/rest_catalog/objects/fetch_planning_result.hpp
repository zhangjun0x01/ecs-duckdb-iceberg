#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class FetchPlanningResult {
public:
	static FetchPlanningResult FromJSON(yyjson_val *obj) {
		FetchPlanningResult result;
		return result;
	}

public:
};
} // namespace rest_api_objects
} // namespace duckdb