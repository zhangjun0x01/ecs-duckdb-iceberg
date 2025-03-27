#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/sort_order.hpp"
#include "rest_catalog/objects/base_update.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class AddSortOrderUpdate {
public:
	static AddSortOrderUpdate FromJSON(yyjson_val *obj) {
		AddSortOrderUpdate result;

		// Parse BaseUpdate fields
		result.base_update = BaseUpdate::FromJSON(obj);

		auto action_val = yyjson_obj_get(obj, "action");
		if (action_val) {
			result.action = yyjson_get_str(action_val);
		}

		auto sort_order_val = yyjson_obj_get(obj, "sort-order");
		if (sort_order_val) {
			result.sort_order = SortOrder::FromJSON(sort_order_val);
		}
		else {
			throw IOException("AddSortOrderUpdate required property 'sort-order' is missing");
		}

		return result;
	}

public:
	BaseUpdate base_update;
	string action;
	SortOrder sort_order;
};
} // namespace rest_api_objects
} // namespace duckdb