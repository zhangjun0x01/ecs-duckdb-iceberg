#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class Transform {
public:
	static Transform FromJSON(yyjson_val *obj) {
		Transform result;

		return result;
	}

public:
};
} // namespace rest_api_objects
} // namespace duckdb