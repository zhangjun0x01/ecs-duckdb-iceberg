#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/expression.hpp"
#include "rest_catalog/objects/metrics.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class ScanReport {
public:
	static ScanReport FromJSON(yyjson_val *obj) {
		ScanReport result;

		auto filter_val = yyjson_obj_get(obj, "filter");
		if (filter_val) {
			result.filter = Expression::FromJSON(filter_val);
		} else {
			throw IOException("ScanReport required property 'filter' is missing");
		}

		auto metadata_val = yyjson_obj_get(obj, "metadata");
		if (metadata_val) {
			result.metadata = parse_object_of_strings(metadata_val);
		}

		auto metrics_val = yyjson_obj_get(obj, "metrics");
		if (metrics_val) {
			result.metrics = Metrics::FromJSON(metrics_val);
		} else {
			throw IOException("ScanReport required property 'metrics' is missing");
		}

		auto projected_field_ids_val = yyjson_obj_get(obj, "projected-field-ids");
		if (projected_field_ids_val) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(projected_field_ids_val, idx, max, val) {
				result.projected_field_ids.push_back(yyjson_get_sint(val));
			}
		} else {
			throw IOException("ScanReport required property 'projected-field-ids' is missing");
		}

		auto projected_field_names_val = yyjson_obj_get(obj, "projected-field-names");
		if (projected_field_names_val) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(projected_field_names_val, idx, max, val) {
				result.projected_field_names.push_back(yyjson_get_str(val));
			}
		} else {
			throw IOException("ScanReport required property 'projected-field-names' is missing");
		}

		auto schema_id_val = yyjson_obj_get(obj, "schema-id");
		if (schema_id_val) {
			result.schema_id = yyjson_get_sint(schema_id_val);
		} else {
			throw IOException("ScanReport required property 'schema-id' is missing");
		}

		auto snapshot_id_val = yyjson_obj_get(obj, "snapshot-id");
		if (snapshot_id_val) {
			result.snapshot_id = yyjson_get_sint(snapshot_id_val);
		} else {
			throw IOException("ScanReport required property 'snapshot-id' is missing");
		}

		auto table_name_val = yyjson_obj_get(obj, "table-name");
		if (table_name_val) {
			result.table_name = yyjson_get_str(table_name_val);
		} else {
			throw IOException("ScanReport required property 'table-name' is missing");
		}

		return result;
	}

public:
	Expression filter;
	case_insensitive_map_t<string> metadata;
	Metrics metrics;
	vector<int64_t> projected_field_ids;
	vector<string> projected_field_names;
	int64_t schema_id;
	int64_t snapshot_id;
	string table_name;
};
} // namespace rest_api_objects
} // namespace duckdb
