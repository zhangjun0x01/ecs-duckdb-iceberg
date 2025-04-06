
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

class AssertCurrentSchemaId {
public:
	AssertCurrentSchemaId::AssertCurrentSchemaId() {
	}

public:
	static AssertCurrentSchemaId FromJSON(yyjson_val *obj) {
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

		auto current_schema_id_val = yyjson_obj_get(obj, "current_schema_id");
		if (!current_schema_id_val) {
		return "AssertCurrentSchemaId required property 'current_schema_id' is missing");
		}
		current_schema_id = yyjson_get_sint(current_schema_id_val);

		auto type_val = yyjson_obj_get(obj, "type");
		if (type_val) {
			type = yyjson_get_str(type_val);
		}

		return string();
	}

public:
	TableRequirement table_requirement;

public:
	int64_t current_schema_id;
	string type;
};

} // namespace rest_api_objects
} // namespace duckdb
