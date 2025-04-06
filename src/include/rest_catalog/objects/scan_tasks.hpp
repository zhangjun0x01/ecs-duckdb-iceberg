
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/delete_file.hpp"
#include "rest_catalog/objects/file_scan_task.hpp"
#include "rest_catalog/objects/plan_task.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class ScanTasks {
public:
	ScanTasks() {
	}

public:
	static ScanTasks FromJSON(yyjson_val *obj) {
		ScanTasks res;
		auto error = res.TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return res;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		auto delete_files_val = yyjson_obj_get(obj, "delete_files");
		if (delete_files_val) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(delete_files_val, idx, max, val) {

				DeleteFile tmp;
				error = tmp.TryFromJSON(val);
				if (!error.empty()) {
					return error;
				}
				delete_files.push_back(tmp);
			}
		}

		auto file_scan_tasks_val = yyjson_obj_get(obj, "file_scan_tasks");
		if (file_scan_tasks_val) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(file_scan_tasks_val, idx, max, val) {

				FileScanTask tmp;
				error = tmp.TryFromJSON(val);
				if (!error.empty()) {
					return error;
				}
				file_scan_tasks.push_back(tmp);
			}
		}

		auto plan_tasks_val = yyjson_obj_get(obj, "plan_tasks");
		if (plan_tasks_val) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(plan_tasks_val, idx, max, val) {

				PlanTask tmp;
				error = tmp.TryFromJSON(val);
				if (!error.empty()) {
					return error;
				}
				plan_tasks.push_back(tmp);
			}
		}

		return string();
	}

public:
public:
	vector<DeleteFile> delete_files;
	vector<FileScanTask> file_scan_tasks;
	vector<PlanTask> plan_tasks;
};

} // namespace rest_api_objects
} // namespace duckdb
