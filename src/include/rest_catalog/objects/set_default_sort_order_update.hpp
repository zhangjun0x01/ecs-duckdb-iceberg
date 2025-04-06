
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

class SetDefaultSortOrderUpdate {
public:
	SetDefaultSortOrderUpdate::SetDefaultSortOrderUpdate() {
	}

public:
	static SetDefaultSortOrderUpdate FromJSON(yyjson_val *obj) {
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

		auto sort_order_id_val = yyjson_obj_get(obj, "sort_order_id");
		if (!sort_order_id_val) {
		return "SetDefaultSortOrderUpdate required property 'sort_order_id' is missing");
		}
		sort_order_id = yyjson_get_sint(sort_order_id_val);

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
	int64_t sort_order_id;
};

} // namespace rest_api_objects
} // namespace duckdb
