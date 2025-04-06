
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

class RemoveSchemasUpdate {
public:
	RemoveSchemasUpdate::RemoveSchemasUpdate() {
	}

public:
	static RemoveSchemasUpdate FromJSON(yyjson_val *obj) {
		auto error = TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return *this;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		error = base_base_update.TryFromJSON(obj);
		if (!error.empty()) {
			return error;
		}

		auto schema_ids_val = yyjson_obj_get(obj, "schema_ids");
		if (!schema_ids_val) {
		return "RemoveSchemasUpdate required property 'schema_ids' is missing");
		}
		size_t idx, max;
		yyjson_val *val;
		yyjson_arr_foreach(schema_ids_val, idx, max, val) {
			result.schema_ids.push_back(yyjson_get_sint(val));
		}

		auto action_val = yyjson_obj_get(obj, "action");
		if (action_val) {
			result.action = yyjson_get_str(action_val);
			;
		}
		return string();
	}

public:
	BaseUpdate base_update;

public:
};

} // namespace rest_api_objects
} // namespace duckdb
