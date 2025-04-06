
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/data_file.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class FileScanTask {
public:
	FileScanTask() {
	}

public:
	static FileScanTask FromJSON(yyjson_val *obj) {
		FileScanTask res;
		auto error = res.TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return res;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		auto data_file_val = yyjson_obj_get(obj, "data_file");
		if (!data_file_val) {
			return "FileScanTask required property 'data_file' is missing";
		} else {
			error = data_file.TryFromJSON(data_file_val);
			if (!error.empty()) {
				return error;
			}
		}

		auto delete_file_references_val = yyjson_obj_get(obj, "delete_file_references");
		if (delete_file_references_val) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(delete_file_references_val, idx, max, val) {

				auto tmp = yyjson_get_sint(val);
				delete_file_references.push_back(tmp);
			}
		}

		auto residual_filter_val = yyjson_obj_get(obj, "residual_filter");
		if (residual_filter_val) {
			residual_filter = residual_filter_val;
		}

		return string();
	}

public:
public:
	DataFile data_file;
	vector<int64_t> delete_file_references;
	yyjson_val *residual_filter;
};

} // namespace rest_api_objects
} // namespace duckdb
