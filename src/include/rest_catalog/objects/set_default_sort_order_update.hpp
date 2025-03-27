#pragma once

#include "yyjson.hpp"
#include <string>
#include <vector>
#include <unordered_map>
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/base_update.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class SetDefaultSortOrderUpdate {
public:
	static SetDefaultSortOrderUpdate FromJSON(yyjson_val *obj) {
		SetDefaultSortOrderUpdate result;

		// Parse BaseUpdate fields
		result.base_update = BaseUpdate::FromJSON(obj);

		auto action_val = yyjson_obj_get(obj, "action");
		if (action_val) {
			result.action = yyjson_get_str(action_val);
		}

		auto sort_order_id_val = yyjson_obj_get(obj, "sort-order-id");
		if (sort_order_id_val) {
			result.sort_order_id = yyjson_get_sint(sort_order_id_val);
		}
		else {
			throw IOException("SetDefaultSortOrderUpdate required property 'sort-order-id' is missing");
		}

		return result;
	}

public:
	BaseUpdate base_update;
	string action;
	int64_t sort_order_id;
};
} // namespace rest_api_objects
} // namespace duckdb