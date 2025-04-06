
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/snapshot_reference.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class SnapshotReferences {
public:
	SnapshotReferences() {
	}

public:
	static SnapshotReferences FromJSON(yyjson_val *obj) {
		SnapshotReferences res;
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
		yyjson_val *key, *val;
		yyjson_obj_foreach(obj, idx, max, key, val) {
			auto key_str = yyjson_get_str(key);
			SnapshotReference tmp;
			error = tmp.TryFromJSON(val);
			if (!error.empty()) {
				return error;
			}
			additional_properties[key_str] = tmp;
		}
		return string();
	}

public:
	case_insensitive_map_t<SnapshotReference> additional_properties;
};

} // namespace rest_api_objects
} // namespace duckdb
