
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/null_order.hpp"
#include "rest_catalog/objects/sort_direction.hpp"
#include "rest_catalog/objects/transform.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class SortField {
public:
	SortField::SortField() {
	}

public:
	static SortField FromJSON(yyjson_val *obj) {
		auto error = TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return *this;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		auto source_id_val = yyjson_obj_get(obj, "source_id");
		if (!source_id_val) {
		return "SortField required property 'source_id' is missing");
		}
		source_id = yyjson_get_sint(source_id_val);

		auto transform_val = yyjson_obj_get(obj, "transform");
		if (!transform_val) {
		return "SortField required property 'transform' is missing");
		}
		error = transform.TryFromJSON(transform_val);
		if (!error.empty()) {
			return error;
		}

		auto direction_val = yyjson_obj_get(obj, "direction");
		if (!direction_val) {
		return "SortField required property 'direction' is missing");
		}
		error = direction.TryFromJSON(direction_val);
		if (!error.empty()) {
			return error;
		}

		auto null_order_val = yyjson_obj_get(obj, "null_order");
		if (!null_order_val) {
		return "SortField required property 'null_order' is missing");
		}
		error = null_order.TryFromJSON(null_order_val);
		if (!error.empty()) {
			return error;
		}

		return string();
	}

public:
public:
	SortDirection direction;
	NullOrder null_order;
	int64_t source_id;
	Transform transform;
};

} // namespace rest_api_objects
} // namespace duckdb
