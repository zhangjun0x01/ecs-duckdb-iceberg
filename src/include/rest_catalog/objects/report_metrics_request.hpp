#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
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
		else {
			throw IOException("ReportMetricsRequest required property 'report-type' is missing");
		}

		return result;
	}

public:
	string report_type;
};
} // namespace rest_api_objects
} // namespace duckdb