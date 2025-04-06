
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/scan_tasks.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class FetchScanTasksResult {
public:
	FetchScanTasksResult::FetchScanTasksResult() {
	}

public:
	static FetchScanTasksResult FromJSON(yyjson_val *obj) {
		auto error = TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return *this;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		error = base_scan_tasks.TryFromJSON(obj);
		if (!error.empty()) {
			return error;
		}

		return string();
	}

public:
	ScanTasks scan_tasks;

public:
};

} // namespace rest_api_objects
} // namespace duckdb
