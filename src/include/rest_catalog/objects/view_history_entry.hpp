#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class ViewHistoryEntry {
public:
	static ViewHistoryEntry FromJSON(yyjson_val *obj) {
		ViewHistoryEntry result;

		auto timestamp_ms_val = yyjson_obj_get(obj, "timestamp-ms");
		if (timestamp_ms_val) {
			result.timestamp_ms = yyjson_get_sint(timestamp_ms_val);
		} else {
			throw IOException("ViewHistoryEntry required property 'timestamp-ms' is missing");
		}

		auto version_id_val = yyjson_obj_get(obj, "version-id");
		if (version_id_val) {
			result.version_id = yyjson_get_sint(version_id_val);
		} else {
			throw IOException("ViewHistoryEntry required property 'version-id' is missing");
		}

		return result;
	}

public:
	int64_t timestamp_ms;
	int64_t version_id;
};
} // namespace rest_api_objects
} // namespace duckdb
