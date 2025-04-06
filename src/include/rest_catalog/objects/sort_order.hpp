
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/sort_field.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class SortOrder {
public:
	SortOrder::SortOrder() {
	}

public:
	static SortOrder FromJSON(yyjson_val *obj) {
		auto error = TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return *this;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		auto order_id_val = yyjson_obj_get(obj, "order_id");
		if (!order_id_val) {
		return "SortOrder required property 'order_id' is missing");
		}
		result.order_id = yyjson_get_sint(order_id_val);

		auto fields_val = yyjson_obj_get(obj, "fields");
		if (!fields_val) {
		return "SortOrder required property 'fields' is missing");
		}
		size_t idx, max;
		yyjson_val *val;
		yyjson_arr_foreach(fields_val, idx, max, val) {
			result.fields.push_back(SortField::FromJSON(val));
		}

		return string();
	}

public:
public:
	vector<SortField> fields;
	int64_t order_id;
};

} // namespace rest_api_objects
} // namespace duckdb
