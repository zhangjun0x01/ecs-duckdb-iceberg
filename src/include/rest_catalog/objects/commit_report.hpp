
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
	CommitReport::CommitReport() {
	}

public:
	static CommitReport FromJSON(yyjson_val *obj) {
		auto error = TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return *this;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		auto table_name_val = yyjson_obj_get(obj, "table_name");
		if (!table_name_val) {
		return "CommitReport required property 'table_name' is missing");
		}
		table_name = yyjson_get_str(table_name_val);

		auto snapshot_id_val = yyjson_obj_get(obj, "snapshot_id");
		if (!snapshot_id_val) {
		return "CommitReport required property 'snapshot_id' is missing");
		}
		snapshot_id = yyjson_get_sint(snapshot_id_val);

		auto sequence_number_val = yyjson_obj_get(obj, "sequence_number");
		if (!sequence_number_val) {
		return "CommitReport required property 'sequence_number' is missing");
		}
		sequence_number = yyjson_get_sint(sequence_number_val);

		auto operation_val = yyjson_obj_get(obj, "operation");
		if (!operation_val) {
		return "CommitReport required property 'operation' is missing");
		}
		operation = yyjson_get_str(operation_val);

		auto metrics_val = yyjson_obj_get(obj, "metrics");
		if (!metrics_val) {
		return "CommitReport required property 'metrics' is missing");
		}
		error = metrics.TryFromJSON(metrics_val);
		if (!error.empty()) {
			return error;
		}

		auto metadata_val = yyjson_obj_get(obj, "metadata");
		if (metadata_val) {
			metadata = parse_object_of_strings(metadata_val);
		}
		return string();
	}

public:
public:
	yyjson_val *metadata;
	Metrics metrics;
	string operation;
	int64_t sequence_number;
	int64_t snapshot_id;
	string table_name;
};

} // namespace rest_api_objects
} // namespace duckdb
