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

class AssertDefaultSortOrderId {
public:
	static AssertDefaultSortOrderId FromJSON(yyjson_val *obj) {
		AssertDefaultSortOrderId result;

		// Parse TableRequirement fields
		result.table_requirement = TableRequirement::FromJSON(obj);

		auto default_sort_order_id_val = yyjson_obj_get(obj, "default-sort-order-id");
		if (default_sort_order_id_val) {
			result.default_sort_order_id = yyjson_get_sint(default_sort_order_id_val);
		} else {
			throw IOException("AssertDefaultSortOrderId required property 'default-sort-order-id' is missing");
		}

		auto type_val = yyjson_obj_get(obj, "type");
		if (type_val) {
			result.type = yyjson_get_str(type_val);
		}

		return result;
	}

public:
	TableRequirement table_requirement;
	int64_t default_sort_order_id;
	string type;
};
} // namespace rest_api_objects
} // namespace duckdb
