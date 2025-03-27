#pragma once

#include "yyjson.hpp"
#include <string>
#include <vector>
#include <unordered_map>
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/table_requirement.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class AssertCurrentSchemaId {
public:
	static AssertCurrentSchemaId FromJSON(yyjson_val *obj) {
		AssertCurrentSchemaId result;

		// Parse TableRequirement fields
		result.table_requirement = TableRequirement::FromJSON(obj);

		auto type_val = yyjson_obj_get(obj, "type");
		if (type_val) {
			result.type = yyjson_get_str(type_val);
		}

		auto current_schema_id_val = yyjson_obj_get(obj, "current-schema-id");
		if (current_schema_id_val) {
			result.current_schema_id = yyjson_get_sint(current_schema_id_val);
		}
		else {
			throw IOException("AssertCurrentSchemaId required property 'current-schema-id' is missing");
		}

		return result;
	}

public:
	TableRequirement table_requirement;
	string type;
	int64_t current_schema_id;
};
} // namespace rest_api_objects
} // namespace duckdb