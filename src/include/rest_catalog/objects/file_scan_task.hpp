#pragma once

#include "yyjson.hpp"
#include <string>
#include <vector>
#include <unordered_map>
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/data_file.hpp"
#include "rest_catalog/objects/expression.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class FileScanTask {
public:
	static FileScanTask FromJSON(yyjson_val *obj) {
		FileScanTask result;

		auto data_file_val = yyjson_obj_get(obj, "data-file");
		if (data_file_val) {
			result.data_file = DataFile::FromJSON(data_file_val);
		}
		else {
			throw IOException("FileScanTask required property 'data-file' is missing");
		}

		auto delete_file_references_val = yyjson_obj_get(obj, "delete-file-references");
		if (delete_file_references_val) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(delete_file_references_val, idx, max, val) {
				result.delete_file_references.push_back(yyjson_get_sint(val));
			}
		}

		auto residual_filter_val = yyjson_obj_get(obj, "residual-filter");
		if (residual_filter_val) {
			result.residual_filter = Expression::FromJSON(residual_filter_val);
		}

		return result;
	}

public:
	DataFile data_file;
	vector<int64_t> delete_file_references;
	Expression residual_filter;
};
} // namespace rest_api_objects
} // namespace duckdb