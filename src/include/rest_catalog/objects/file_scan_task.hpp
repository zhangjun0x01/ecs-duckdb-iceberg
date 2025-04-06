
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
	FileScanTask::FileScanTask() {
	}

public:
	static FileScanTask FromJSON(yyjson_val *obj) {
		auto error = TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return *this;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		auto data_file_val = yyjson_obj_get(obj, "data_file");
		if (!data_file_val) {
		return "FileScanTask required property 'data_file' is missing");
		}
		result.data_file = DataFile::FromJSON(data_file_val);

		auto delete_file_references_val = yyjson_obj_get(obj, "delete_file_references");
		if (delete_file_references_val) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(delete_file_references_val, idx, max, val) {
				result.delete_file_references.push_back(yyjson_get_sint(val));
			};
		}

		auto residual_filter_val = yyjson_obj_get(obj, "residual_filter");
		if (residual_filter_val) {
			result.residual_filter = residual_filter_val;
			;
		}
		return string();
	}

public:
public:
};

} // namespace rest_api_objects
} // namespace duckdb
