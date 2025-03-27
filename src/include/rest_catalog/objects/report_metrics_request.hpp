#pragma once

#include "yyjson.hpp"
#include <string>
#include <vector>
#include <unordered_map>
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class ReportMetricsRequest {
public:
	static ReportMetricsRequest FromJSON(yyjson_val *obj) {
		ReportMetricsRequest result;
		auto report_type_val = yyjson_obj_get(obj, "report-type");
		if (report_type_val) {
			result.report_type = yyjson_get_str(report_type_val);
		}
		return result;
	}
public:
	string report_type;
};

} // namespace rest_api_objects
} // namespace duckdb