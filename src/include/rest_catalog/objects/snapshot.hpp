
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
	Snapshot() {
	}

public:
	class Object2 {
	public:
		Object2() {
		}

	public:
		static Object2 FromJSON(yyjson_val *obj) {
			Object2 res;
			auto error = res.TryFromJSON(obj);
			if (!error.empty()) {
				throw InvalidInputException(error);
			}
			return res;
		}

	public:
		string TryFromJSON(yyjson_val *obj) {
			string error;

			auto operation_val = yyjson_obj_get(obj, "operation");
			if (!operation_val) {
				return "Object2 required property 'operation' is missing";
			} else {
				operation = yyjson_get_str(operation_val);
			}

			case_insensitive_set_t handled_properties {"operation"};

			size_t idx, max;
			yyjson_val *key, *val;
			yyjson_obj_foreach(obj, idx, max, key, val) {
				auto key_str = yyjson_get_str(key);
				if (handled_properties.count(key_str)) {
					continue;
				}
				auto tmp = yyjson_get_str(val);

				additional_properties[key_str] = tmp;
			}

			return string();
		}

	public:
	public:
		string operation;
		case_insensitive_map_t<string> additional_properties;
	};

public:
	static Snapshot FromJSON(yyjson_val *obj) {
		Snapshot res;
		auto error = res.TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return res;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		auto snapshot_id_val = yyjson_obj_get(obj, "snapshot_id");
		if (!snapshot_id_val) {
			return "Snapshot required property 'snapshot_id' is missing";
		} else {
			snapshot_id = yyjson_get_sint(snapshot_id_val);
		}

		auto timestamp_ms_val = yyjson_obj_get(obj, "timestamp_ms");
		if (!timestamp_ms_val) {
			return "Snapshot required property 'timestamp_ms' is missing";
		} else {
			timestamp_ms = yyjson_get_sint(timestamp_ms_val);
		}

		auto manifest_list_val = yyjson_obj_get(obj, "manifest_list");
		if (!manifest_list_val) {
			return "Snapshot required property 'manifest_list' is missing";
		} else {
			manifest_list = yyjson_get_str(manifest_list_val);
		}

		auto summary_val = yyjson_obj_get(obj, "summary");
		if (!summary_val) {
			return "Snapshot required property 'summary' is missing";
		} else {
			error = summary.TryFromJSON(summary_val);
			if (!error.empty()) {
				return error;
			}
		}

		auto parent_snapshot_id_val = yyjson_obj_get(obj, "parent_snapshot_id");
		if (parent_snapshot_id_val) {
			parent_snapshot_id = yyjson_get_sint(parent_snapshot_id_val);
		}

		auto sequence_number_val = yyjson_obj_get(obj, "sequence_number");
		if (sequence_number_val) {
			sequence_number = yyjson_get_sint(sequence_number_val);
		}

		auto schema_id_val = yyjson_obj_get(obj, "schema_id");
		if (schema_id_val) {
			schema_id = yyjson_get_sint(schema_id_val);
		}

		return string();
	}

public:
public:
	string manifest_list;
	int64_t parent_snapshot_id;
	int64_t schema_id;
	int64_t sequence_number;
	int64_t snapshot_id;
	Object2 summary;
	int64_t timestamp_ms;
};

} // namespace rest_api_objects
} // namespace duckdb
