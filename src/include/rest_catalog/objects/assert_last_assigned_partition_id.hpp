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

class AssertLastAssignedPartitionId {
public:
	static AssertLastAssignedPartitionId FromJSON(yyjson_val *obj) {
		AssertLastAssignedPartitionId result;

		// Parse TableRequirement fields
		result.table_requirement = TableRequirement::FromJSON(obj);

		auto last_assigned_partition_id_val = yyjson_obj_get(obj, "last-assigned-partition-id");
		if (last_assigned_partition_id_val) {
			result.last_assigned_partition_id = yyjson_get_sint(last_assigned_partition_id_val);
		} else {
			throw IOException(
			    "AssertLastAssignedPartitionId required property 'last-assigned-partition-id' is missing");
		}

		auto type_val = yyjson_obj_get(obj, "type");
		if (type_val) {
			result.type = yyjson_get_str(type_val);
		}

		return result;
	}

public:
	TableRequirement table_requirement;
	int64_t last_assigned_partition_id;
	string type;
};
} // namespace rest_api_objects
} // namespace duckdb
