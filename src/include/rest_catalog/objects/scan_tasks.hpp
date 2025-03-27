#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/delete_file.hpp"
#include "rest_catalog/objects/file_scan_task.hpp"
#include "rest_catalog/objects/plan_task.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class ScanTasks {
public:
	static ScanTasks FromJSON(yyjson_val *obj) {
		ScanTasks result;

		auto delete_files_val = yyjson_obj_get(obj, "delete-files");
		if (delete_files_val) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(delete_files_val, idx, max, val) {
				result.delete_files.push_back(DeleteFile::FromJSON(val));
			}
		}

		auto file_scan_tasks_val = yyjson_obj_get(obj, "file-scan-tasks");
		if (file_scan_tasks_val) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(file_scan_tasks_val, idx, max, val) {
				result.file_scan_tasks.push_back(FileScanTask::FromJSON(val));
			}
		}

		auto plan_tasks_val = yyjson_obj_get(obj, "plan-tasks");
		if (plan_tasks_val) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(plan_tasks_val, idx, max, val) {
				result.plan_tasks.push_back(PlanTask::FromJSON(val));
			}
		}

		return result;
	}

public:
	vector<DeleteFile> delete_files;
	vector<FileScanTask> file_scan_tasks;
	vector<PlanTask> plan_tasks;
};
} // namespace rest_api_objects
} // namespace duckdb