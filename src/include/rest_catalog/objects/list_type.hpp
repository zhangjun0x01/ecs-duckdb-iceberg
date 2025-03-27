#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/type.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class ListType {
public:
	static ListType FromJSON(yyjson_val *obj) {
		ListType result;

		auto type_val = yyjson_obj_get(obj, "type");
		if (type_val) {
			result.type = yyjson_get_str(type_val);
		}
		else {
			throw IOException("ListType required property 'type' is missing");
		}

		auto element_id_val = yyjson_obj_get(obj, "element-id");
		if (element_id_val) {
			result.element_id = yyjson_get_sint(element_id_val);
		}
		else {
			throw IOException("ListType required property 'element-id' is missing");
		}

		auto element_val = yyjson_obj_get(obj, "element");
		if (element_val) {
			result.element = Type::FromJSON(element_val);
		}
		else {
			throw IOException("ListType required property 'element' is missing");
		}

		auto element_required_val = yyjson_obj_get(obj, "element-required");
		if (element_required_val) {
			result.element_required = yyjson_get_bool(element_required_val);
		}
		else {
			throw IOException("ListType required property 'element-required' is missing");
		}

		return result;
	}

public:
	string type;
	int64_t element_id;
	Type element;
	bool element_required;
};
} // namespace rest_api_objects
} // namespace duckdb