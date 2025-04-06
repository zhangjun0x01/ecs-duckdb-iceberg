
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/content_file.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class DataFile {
public:
	DataFile();

public:
	static DataFile FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	ContentFile content_file;
	string content;
	yyjson_val *column_sizes;
	yyjson_val *value_counts;
	yyjson_val *null_value_counts;
	yyjson_val *nan_value_counts;
	yyjson_val *lower_bounds;
	yyjson_val *upper_bounds;
};

} // namespace rest_api_objects
} // namespace duckdb
