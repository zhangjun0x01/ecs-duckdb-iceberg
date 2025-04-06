
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
	ViewHistoryEntry::ViewHistoryEntry() {
	}

public:
	static ViewHistoryEntry FromJSON(yyjson_val *obj) {
		auto error = TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return *this;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		auto version_id_val = yyjson_obj_get(obj, "version_id");
		if (!version_id_val) {
		return "ViewHistoryEntry required property 'version_id' is missing");
		}
		result.version_id = yyjson_get_sint(version_id_val);

		auto timestamp_ms_val = yyjson_obj_get(obj, "timestamp_ms");
		if (!timestamp_ms_val) {
		return "ViewHistoryEntry required property 'timestamp_ms' is missing");
		}
		result.timestamp_ms = yyjson_get_sint(timestamp_ms_val);

		return string();
	}

public:
public:
	int64_t version_id;
	int64_t timestamp_ms;
};

} // namespace rest_api_objects
} // namespace duckdb
