
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
	AssertDefaultSortOrderId::AssertDefaultSortOrderId() {
	}

public:
	static AssertDefaultSortOrderId FromJSON(yyjson_val *obj) {
		auto error = TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return *this;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		error = base_table_requirement.TryFromJSON(obj);
		if (!error.empty()) {
			return error;
		}

		auto default_sort_order_id_val = yyjson_obj_get(obj, "default_sort_order_id");
		if (!default_sort_order_id_val) {
		return "AssertDefaultSortOrderId required property 'default_sort_order_id' is missing");
		}
		result.default_sort_order_id = yyjson_get_sint(default_sort_order_id_val);

		auto type_val = yyjson_obj_get(obj, "type");
		if (type_val) {
			result.type = yyjson_get_str(type_val);
			;
		}
		return string();
	}

public:
	TableRequirement table_requirement;

public:
};

} // namespace rest_api_objects
} // namespace duckdb
