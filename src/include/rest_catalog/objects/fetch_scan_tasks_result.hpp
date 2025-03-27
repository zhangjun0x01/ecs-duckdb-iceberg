#pragma once

#include "yyjson.hpp"
#include <string>
#include <vector>
#include <unordered_map>
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/scan_tasks.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class FetchScanTasksResult {
public:
	static FetchScanTasksResult FromJSON(yyjson_val *obj) {
		FetchScanTasksResult result;
		return result;
	}
public:
};

} // namespace rest_api_objects
} // namespace duckdb