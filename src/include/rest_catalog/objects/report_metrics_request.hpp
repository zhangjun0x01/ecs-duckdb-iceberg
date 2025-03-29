#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class ReportMetricsRequest {
public:
	static ReportMetricsRequest FromJSON(yyjson_val *obj) {
		ReportMetricsRequest result;
		if (yyjson_is_obj(obj)) {
			if (yyjson_obj_get(obj, "filter") && yyjson_obj_get(obj, "metrics") &&
			    yyjson_obj_get(obj, "projected-field-ids") && yyjson_obj_get(obj, "projected-field-names") &&
			    yyjson_obj_get(obj, "schema-id") && yyjson_obj_get(obj, "snapshot-id") &&
			    yyjson_obj_get(obj, "table-name")) {
				result.scan_report = ScanReport::FromJSON(obj);
				result.has_scan_report = true;
			}
			if (yyjson_obj_get(obj, "metrics") && yyjson_obj_get(obj, "operation") &&
			    yyjson_obj_get(obj, "sequence-number") && yyjson_obj_get(obj, "snapshot-id") &&
			    yyjson_obj_get(obj, "table-name")) {
				result.commit_report = CommitReport::FromJSON(obj);
				result.has_commit_report = true;
			}
			if (!(result.has_scan_report || result.has_commit_report)) {
				throw IOException("ReportMetricsRequest failed to parse, none of the accepted schemas found");
			}
		} else {
			throw IOException("ReportMetricsRequest must be an object");
		}
		return result;
	}

public:
	ScanReport scan_report;
	bool has_scan_report = false;
	CommitReport commit_report;
	bool has_commit_report = false;
};
} // namespace rest_api_objects
} // namespace duckdb
