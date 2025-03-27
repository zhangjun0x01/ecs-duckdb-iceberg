#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/table_requirement.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class AssertLastAssignedFieldId {
public:
	static AssertLastAssignedFieldId FromJSON(yyjson_val *obj) {
		AssertLastAssignedFieldId result;

		// Parse TableRequirement fields
		result.table_requirement = TableRequirement::FromJSON(obj);

		auto type_val = yyjson_obj_get(obj, "type");
		if (type_val) {
			result.type = yyjson_get_str(type_val);
		}

		auto last_assigned_field_id_val = yyjson_obj_get(obj, "last-assigned-field-id");
		if (last_assigned_field_id_val) {
			result.last_assigned_field_id = yyjson_get_sint(last_assigned_field_id_val);
		}
		else {
			throw IOException("AssertLastAssignedFieldId required property 'last-assigned-field-id' is missing");
		}

		return result;
	}

public:
	TableRequirement table_requirement;
	string type;
	int64_t last_assigned_field_id;
};
} // namespace rest_api_objects
} // namespace duckdb