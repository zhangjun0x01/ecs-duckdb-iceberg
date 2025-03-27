#pragma once

#include "yyjson.hpp"
#include <string>
#include <vector>
#include <unordered_map>
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/content_file.hpp"
#include "rest_catalog/objects/count_map.hpp"
#include "rest_catalog/objects/value_map.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class DataFile {
public:
	static DataFile FromJSON(yyjson_val *obj) {
		DataFile result;
		auto content_val = yyjson_obj_get(obj, "content");
		if (content_val) {
			result.content = yyjson_get_str(content_val);
		}
		auto column_sizes_val = yyjson_obj_get(obj, "column-sizes");
		if (column_sizes_val) {
			result.column_sizes = CountMap::FromJSON(column_sizes_val);
		}
		auto value_counts_val = yyjson_obj_get(obj, "value-counts");
		if (value_counts_val) {
			result.value_counts = CountMap::FromJSON(value_counts_val);
		}
		auto null_value_counts_val = yyjson_obj_get(obj, "null-value-counts");
		if (null_value_counts_val) {
			result.null_value_counts = CountMap::FromJSON(null_value_counts_val);
		}
		auto nan_value_counts_val = yyjson_obj_get(obj, "nan-value-counts");
		if (nan_value_counts_val) {
			result.nan_value_counts = CountMap::FromJSON(nan_value_counts_val);
		}
		auto lower_bounds_val = yyjson_obj_get(obj, "lower-bounds");
		if (lower_bounds_val) {
			result.lower_bounds = ValueMap::FromJSON(lower_bounds_val);
		}
		auto upper_bounds_val = yyjson_obj_get(obj, "upper-bounds");
		if (upper_bounds_val) {
			result.upper_bounds = ValueMap::FromJSON(upper_bounds_val);
		}
		return result;
	}
public:
	string content;
	CountMap column_sizes;
	CountMap value_counts;
	CountMap null_value_counts;
	CountMap nan_value_counts;
	ValueMap lower_bounds;
	ValueMap upper_bounds;
};

} // namespace rest_api_objects
} // namespace duckdb