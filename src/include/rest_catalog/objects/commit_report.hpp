#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/metrics.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class CommitReport {
public:
	static CommitReport FromJSON(yyjson_val *obj) {
		CommitReport result;

		auto metadata_val = yyjson_obj_get(obj, "metadata");
		if (metadata_val) {
			result.metadata = parse_object_of_strings(metadata_val);
		}

		auto metrics_val = yyjson_obj_get(obj, "metrics");
		if (metrics_val) {
			result.metrics = Metrics::FromJSON(metrics_val);
		}
		else {
			throw IOException("CommitReport required property 'metrics' is missing");
		}

		auto operation_val = yyjson_obj_get(obj, "operation");
		if (operation_val) {
			result.operation = yyjson_get_str(operation_val);
		}
		else {
			throw IOException("CommitReport required property 'operation' is missing");
		}

		auto sequence_number_val = yyjson_obj_get(obj, "sequence-number");
		if (sequence_number_val) {
			result.sequence_number = yyjson_get_sint(sequence_number_val);
		}
		else {
			throw IOException("CommitReport required property 'sequence-number' is missing");
		}

		auto snapshot_id_val = yyjson_obj_get(obj, "snapshot-id");
		if (snapshot_id_val) {
			result.snapshot_id = yyjson_get_sint(snapshot_id_val);
		}
		else {
			throw IOException("CommitReport required property 'snapshot-id' is missing");
		}

		auto table_name_val = yyjson_obj_get(obj, "table-name");
		if (table_name_val) {
			result.table_name = yyjson_get_str(table_name_val);
		}
		else {
			throw IOException("CommitReport required property 'table-name' is missing");
		}

		return result;
	}

public:
	case_insensitive_map_t<string> metadata;
	Metrics metrics;
	string operation;
	int64_t sequence_number;
	int64_t snapshot_id;
	string table_name;
};
} // namespace rest_api_objects
} // namespace duckdb