
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class Snapshot {
public:
	Snapshot::Snapshot() {
	}

public:
	class Object2 {
	public:
		Object2::Object2() {
		}

	public:
		static Object2 FromJSON(yyjson_val *obj) {
			auto error = TryFromJSON(obj);
			if (!error.empty()) {
				throw InvalidInputException(error);
			}
			return *this;
		}

	public:
		string TryFromJSON(yyjson_val *obj) {
			string error;

			auto operation_val = yyjson_obj_get(obj, "operation");
			if (!operation_val) {
			return "Object2 required property 'operation' is missing");
			}
			result.operation = yyjson_get_str(operation_val);

			return string();
		}

	public:
	public:
		string operation;
	};

public:
	static Snapshot FromJSON(yyjson_val *obj) {
		auto error = TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return *this;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		auto snapshot_id_val = yyjson_obj_get(obj, "snapshot_id");
		if (!snapshot_id_val) {
		return "Snapshot required property 'snapshot_id' is missing");
		}
		result.snapshot_id = yyjson_get_sint(snapshot_id_val);

		auto timestamp_ms_val = yyjson_obj_get(obj, "timestamp_ms");
		if (!timestamp_ms_val) {
		return "Snapshot required property 'timestamp_ms' is missing");
		}
		result.timestamp_ms = yyjson_get_sint(timestamp_ms_val);

		auto manifest_list_val = yyjson_obj_get(obj, "manifest_list");
		if (!manifest_list_val) {
		return "Snapshot required property 'manifest_list' is missing");
		}
		result.manifest_list = yyjson_get_str(manifest_list_val);

		auto summary_val = yyjson_obj_get(obj, "summary");
		if (!summary_val) {
		return "Snapshot required property 'summary' is missing");
		}
		result.summary = Object2::FromJSON(summary_val);

		auto parent_snapshot_id_val = yyjson_obj_get(obj, "parent_snapshot_id");
		if (parent_snapshot_id_val) {
			result.parent_snapshot_id = yyjson_get_sint(parent_snapshot_id_val);
		}

		auto sequence_number_val = yyjson_obj_get(obj, "sequence_number");
		if (sequence_number_val) {
			result.sequence_number = yyjson_get_sint(sequence_number_val);
		}

		auto schema_id_val = yyjson_obj_get(obj, "schema_id");
		if (schema_id_val) {
			result.schema_id = yyjson_get_sint(schema_id_val);
		}
		return string();
	}

public:
public:
	int64_t snapshot_id;
	int64_t parent_snapshot_id;
	int64_t sequence_number;
	int64_t timestamp_ms;
	string manifest_list;
	Object2 summary;
	int64_t schema_id;
};

} // namespace rest_api_objects
} // namespace duckdb
