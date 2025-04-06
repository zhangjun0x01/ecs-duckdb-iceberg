
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/base_update.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class SetCurrentSchemaUpdate {
public:
	SetCurrentSchemaUpdate() {
	}

public:
	static SetCurrentSchemaUpdate FromJSON(yyjson_val *obj) {
		SetCurrentSchemaUpdate res;
		auto error = res.TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return res;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;
		error = base_update.TryFromJSON(obj);
		if (!error.empty()) {
			return error;
		}

		auto schema_id_val = yyjson_obj_get(obj, "schema_id");
		if (!schema_id_val) {
			return "SetCurrentSchemaUpdate required property 'schema_id' is missing";
		} else {
			schema_id = yyjson_get_sint(schema_id_val);
		}

		auto action_val = yyjson_obj_get(obj, "action");
		if (action_val) {
			action = yyjson_get_str(action_val);
		}

		return string();
	}

public:
	BaseUpdate base_update;

public:
	string action;
	int64_t schema_id;
};

} // namespace rest_api_objects
} // namespace duckdb
