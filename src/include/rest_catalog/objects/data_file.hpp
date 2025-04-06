
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/content_file.hpp"
#include "rest_catalog/objects/count_map.hpp"
#include "rest_catalog/objects/value_map.hpp"

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
	CountMap column_sizes;
	CountMap value_counts;
	CountMap null_value_counts;
	CountMap nan_value_counts;
	ValueMap lower_bounds;
	ValueMap upper_bounds;
};

} // namespace rest_api_objects
} // namespace duckdb
