#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class ViewRequirement {
public:
	static ViewRequirement FromJSON(yyjson_val *obj) {
		ViewRequirement result;
		result.assert_view_uuid = AssertViewUUID::FromJSON(obj);
		result.has_assert_view_uuid = true;
		return result;
	}

public:
	AssertViewUUID assert_view_uuid;
	bool has_assert_view_uuid = false;
};
} // namespace rest_api_objects
} // namespace duckdb
