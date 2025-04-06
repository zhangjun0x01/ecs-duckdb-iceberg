
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
	AssertLastAssignedPartitionId::AssertLastAssignedPartitionId() {
	}

public:
	static AssertLastAssignedPartitionId FromJSON(yyjson_val *obj) {
		auto error = TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return *this;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		error = base_table_requirement.TryFromJSON(obj);
		if (!error.empty()) {
			return error;
		}

		auto last_assigned_partition_id_val = yyjson_obj_get(obj, "last_assigned_partition_id");
		if (!last_assigned_partition_id_val) {
		return "AssertLastAssignedPartitionId required property 'last_assigned_partition_id' is missing");
		}
		result.last_assigned_partition_id = yyjson_get_sint(last_assigned_partition_id_val);

		auto type_val = yyjson_obj_get(obj, "type");
		if (type_val) {
			result.type = yyjson_get_str(type_val);
			;
		}
		return string();
	}

public:
	TableRequirement table_requirement;

public:
};

} // namespace rest_api_objects
} // namespace duckdb
