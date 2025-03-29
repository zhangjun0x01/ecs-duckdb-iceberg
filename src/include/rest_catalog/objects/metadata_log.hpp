#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class MetadataLog {
public:
	static MetadataLog FromJSON(yyjson_val *obj) {
		MetadataLog result;
		size_t idx, max;
		yyjson_val *val;
		yyjson_arr_foreach(obj, idx, max, val) {
			result.value.push_back(val);
		}
		return result;
	}

public:
	vector<yyjson_val *> value;
};
} // namespace rest_api_objects
} // namespace duckdb
