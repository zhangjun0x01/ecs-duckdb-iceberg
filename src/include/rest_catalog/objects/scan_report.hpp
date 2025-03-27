#pragma once

#include "yyjson.hpp"
#include <string>
#include <vector>
#include <unordered_map>
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/metrics.hpp"
#include "rest_catalog/objects/expression.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class ScanReport {
public:
	static ScanReport FromJSON(yyjson_val *obj) {
		ScanReport result;
		auto table_name_val = yyjson_obj_get(obj, "table-name");
		if (table_name_val) {
			result.table_name = yyjson_get_str(table_name_val);
		}
		auto snapshot_id_val = yyjson_obj_get(obj, "snapshot-id");
		if (snapshot_id_val) {
			result.snapshot_id = yyjson_get_sint(snapshot_id_val);
		}
		auto filter_val = yyjson_obj_get(obj, "filter");
		if (filter_val) {
			result.filter = Expression::FromJSON(filter_val);
		}
		auto schema_id_val = yyjson_obj_get(obj, "schema-id");
		if (schema_id_val) {
			result.schema_id = yyjson_get_sint(schema_id_val);
		}
		auto projected_field_ids_val = yyjson_obj_get(obj, "projected-field-ids");
		if (projected_field_ids_val) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(projected_field_ids_val, idx, max, val) {
				result.projected_field_ids.push_back(yyjson_get_sint(val));
			}
		}
		auto projected_field_names_val = yyjson_obj_get(obj, "projected-field-names");
		if (projected_field_names_val) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(projected_field_names_val, idx, max, val) {
				result.projected_field_names.push_back(yyjson_get_str(val));
			}
		}
		auto metrics_val = yyjson_obj_get(obj, "metrics");
		if (metrics_val) {
			result.metrics = Metrics::FromJSON(metrics_val);
		}
		auto metadata_val = yyjson_obj_get(obj, "metadata");
		if (metadata_val) {
			result.metadata = parse_object_of_strings(metadata_val);
		}
		return result;
	}
public:
	string table_name;
	int64_t snapshot_id;
	Expression filter;
	int64_t schema_id;
	vector<int64_t> projected_field_ids;
	vector<string> projected_field_names;
	Metrics metrics;
	ObjectOfStrings metadata;
};

} // namespace rest_api_objects
} // namespace duckdb