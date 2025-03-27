#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/sort_field.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class SortOrder {
public:
	static SortOrder FromJSON(yyjson_val *obj) {
		SortOrder result;

		auto order_id_val = yyjson_obj_get(obj, "order-id");
		if (order_id_val) {
			result.order_id = yyjson_get_sint(order_id_val);
		}
		else {
			throw IOException("SortOrder required property 'order-id' is missing");
		}

		auto fields_val = yyjson_obj_get(obj, "fields");
		if (fields_val) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(fields_val, idx, max, val) {
				result.fields.push_back(SortField::FromJSON(val));
			}
		}
		else {
			throw IOException("SortOrder required property 'fields' is missing");
		}

		return result;
	}

public:
	int64_t order_id;
	vector<SortField> fields;
};
} // namespace rest_api_objects
} // namespace duckdb