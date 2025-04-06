
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
	DataFile::DataFile() {
	}

public:
	static DataFile FromJSON(yyjson_val *obj) {
		auto error = TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return *this;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		error = content_file.TryFromJSON(obj);
		if (!error.empty()) {
			return error;
		}

		auto content_val = yyjson_obj_get(obj, "content");
		if (!content_val) {
		return "DataFile required property 'content' is missing");
		}
		content = yyjson_get_str(content_val);

		auto column_sizes_val = yyjson_obj_get(obj, "column_sizes");
		if (column_sizes_val) {
			column_sizes = column_sizes_val;
		}

		auto value_counts_val = yyjson_obj_get(obj, "value_counts");
		if (value_counts_val) {
			value_counts = value_counts_val;
		}

		auto null_value_counts_val = yyjson_obj_get(obj, "null_value_counts");
		if (null_value_counts_val) {
			null_value_counts = null_value_counts_val;
		}

		auto nan_value_counts_val = yyjson_obj_get(obj, "nan_value_counts");
		if (nan_value_counts_val) {
			nan_value_counts = nan_value_counts_val;
		}

		auto lower_bounds_val = yyjson_obj_get(obj, "lower_bounds");
		if (lower_bounds_val) {
			lower_bounds = lower_bounds_val;
		}

		auto upper_bounds_val = yyjson_obj_get(obj, "upper_bounds");
		if (upper_bounds_val) {
			upper_bounds = upper_bounds_val;
		}

		return string();
	}

public:
	ContentFile content_file;

public:
	yyjson_val *column_sizes;
	string content;
	yyjson_val *lower_bounds;
	yyjson_val *nan_value_counts;
	yyjson_val *null_value_counts;
	yyjson_val *upper_bounds;
	yyjson_val *value_counts;
};

} // namespace rest_api_objects
} // namespace duckdb
