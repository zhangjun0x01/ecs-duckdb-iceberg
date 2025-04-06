
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/table_requirement.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class AssertRefSnapshotId {
public:
	AssertRefSnapshotId::AssertRefSnapshotId() {
	}

public:
	static AssertRefSnapshotId FromJSON(yyjson_val *obj) {
		auto error = TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return *this;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		error = table_requirement.TryFromJSON(obj);
		if (!error.empty()) {
			return error;
		}

		auto ref_val = yyjson_obj_get(obj, "ref");
		if (!ref_val) {
		return "AssertRefSnapshotId required property 'ref' is missing");
		}
		ref = yyjson_get_str(ref_val);

		auto snapshot_id_val = yyjson_obj_get(obj, "snapshot_id");
		if (!snapshot_id_val) {
		return "AssertRefSnapshotId required property 'snapshot_id' is missing");
		}
		snapshot_id = yyjson_get_sint(snapshot_id_val);

		auto type_val = yyjson_obj_get(obj, "type");
		if (type_val) {
			type = yyjson_get_str(type_val);
		}
		return string();
	}

public:
	TableRequirement table_requirement;

public:
	string ref;
	int64_t snapshot_id;
	string type;
};

} // namespace rest_api_objects
} // namespace duckdb
