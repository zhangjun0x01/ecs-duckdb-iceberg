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
	static AssertRefSnapshotId FromJSON(yyjson_val *obj) {
		AssertRefSnapshotId result;

		// Parse TableRequirement fields
		result.table_requirement = TableRequirement::FromJSON(obj);

		auto ref_val = yyjson_obj_get(obj, "ref");
		if (ref_val) {
			result.ref = yyjson_get_str(ref_val);
		}
		else {
			throw IOException("AssertRefSnapshotId required property 'ref' is missing");
		}

		auto snapshot_id_val = yyjson_obj_get(obj, "snapshot-id");
		if (snapshot_id_val) {
			result.snapshot_id = yyjson_get_sint(snapshot_id_val);
		}
		else {
			throw IOException("AssertRefSnapshotId required property 'snapshot-id' is missing");
		}

		auto type_val = yyjson_obj_get(obj, "type");
		if (type_val) {
			result.type = yyjson_get_str(type_val);
		}

		return result;
	}

public:
	TableRequirement table_requirement;
	string ref;
	int64_t snapshot_id;
	string type;
};
} // namespace rest_api_objects
} // namespace duckdb