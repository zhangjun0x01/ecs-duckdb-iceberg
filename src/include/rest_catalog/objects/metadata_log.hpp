
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class MetadataLog {
public:
	MetadataLog() {
	}

public:
	class Object4 {
	public:
		Object4() {
		}

	public:
		static Object4 FromJSON(yyjson_val *obj) {
			Object4 res;
			auto error = res.TryFromJSON(obj);
			if (!error.empty()) {
				throw InvalidInputException(error);
			}
			return res;
		}

	public:
		string TryFromJSON(yyjson_val *obj) {
			string error;

			auto metadata_file_val = yyjson_obj_get(obj, "metadata_file");
			if (!metadata_file_val) {
				return "Object4 required property 'metadata_file' is missing";
			} else {
				metadata_file = yyjson_get_str(metadata_file_val);
			}

			auto timestamp_ms_val = yyjson_obj_get(obj, "timestamp_ms");
			if (!timestamp_ms_val) {
				return "Object4 required property 'timestamp_ms' is missing";
			} else {
				timestamp_ms = yyjson_get_sint(timestamp_ms_val);
			}

			return string();
		}

	public:
	public:
		string metadata_file;
		int64_t timestamp_ms;
	};

public:
	static MetadataLog FromJSON(yyjson_val *obj) {
		MetadataLog res;
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

			Object4 tmp;
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
	vector<Object4> value;
};

} // namespace rest_api_objects
} // namespace duckdb
