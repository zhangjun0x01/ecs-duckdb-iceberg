
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/base_update.hpp"
#include "rest_catalog/objects/schema.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class AddSchemaUpdate {
public:
	AddSchemaUpdate::AddSchemaUpdate() {
	}

public:
	static AddSchemaUpdate FromJSON(yyjson_val *obj) {
		auto error = TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return *this;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		error = base_update.TryFromJSON(obj);
		if (!error.empty()) {
			return error;
		}

		auto schema_val = yyjson_obj_get(obj, "schema");
		if (!schema_val) {
		return "AddSchemaUpdate required property 'schema' is missing");
		}
		error = schema.TryFromJSON(schema_val);
		if (!error.empty()) {
			return error;
		}

		auto action_val = yyjson_obj_get(obj, "action");
		if (action_val) {
			action = yyjson_get_str(action_val);
		}

		auto last_column_id_val = yyjson_obj_get(obj, "last_column_id");
		if (last_column_id_val) {
			last_column_id = yyjson_get_sint(last_column_id_val);
		}
		return string();
	}

public:
	BaseUpdate base_update;

public:
	string action;
	int64_t last_column_id;
	Schema schema;
};

} // namespace rest_api_objects
} // namespace duckdb
