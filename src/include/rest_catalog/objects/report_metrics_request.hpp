
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/commit_report.hpp"
#include "rest_catalog/objects/scan_report.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class ReportMetricsRequest {
public:
	ReportMetricsRequest::ReportMetricsRequest() {
	}

public:
	static ReportMetricsRequest FromJSON(yyjson_val *obj) {
		auto error = TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return *this;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		error = base_scan_report.TryFromJSON(obj);
		if (error.empty()) {
			has_scan_report = true;
		}

		error = base_commit_report.TryFromJSON(obj);
		if (error.empty()) {
			has_commit_report = true;
		}

		if (!has_commit_report && !has_scan_report) {
			return "ReportMetricsRequest failed to parse, none of the anyOf candidates matched";
		}

		auto report_type_val = yyjson_obj_get(obj, "report_type");
		if (!report_type_val) {
		return "ReportMetricsRequest required property 'report_type' is missing");
		}
		result.report_type = yyjson_get_str(report_type_val);

		return string();
	}

public:
	ScanReport scan_report;
	CommitReport commit_report;

public:
};

} // namespace rest_api_objects
} // namespace duckdb
