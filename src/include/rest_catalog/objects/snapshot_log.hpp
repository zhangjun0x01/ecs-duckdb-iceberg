
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class SnapshotLog {
public:
	SnapshotLog() {
	}

public:
	class Object3 {
	public:
		Object3() {
		}

	public:
		static Object3 FromJSON(yyjson_val *obj) {
			Object3 res;
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
				return "Object3 required property 'snapshot_id' is missing";
			} else {
				snapshot_id = yyjson_get_sint(snapshot_id_val);
			}

			auto timestamp_ms_val = yyjson_obj_get(obj, "timestamp_ms");
			if (!timestamp_ms_val) {
				return "Object3 required property 'timestamp_ms' is missing";
			} else {
				timestamp_ms = yyjson_get_sint(timestamp_ms_val);
			}

			return string();
		}

	public:
	public:
		int64_t snapshot_id;
		int64_t timestamp_ms;
	};

public:
	static SnapshotLog FromJSON(yyjson_val *obj) {
		SnapshotLog res;
		auto error = res.TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return res;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		size_t idx, max;
		yyjson_val *val;
		yyjson_arr_foreach(obj, idx, max, val) {

			Object3 tmp;
			error = tmp.TryFromJSON(val);
			if (!error.empty()) {
				return error;
			}
			value.push_back(tmp);
		}

		return string();
	}

public:
public:
	vector<Object3> value;
};

} // namespace rest_api_objects
} // namespace duckdb
