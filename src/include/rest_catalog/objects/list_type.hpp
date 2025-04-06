
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/type.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class ListType {
public:
	ListType::ListType() {
	}

public:
	static ListType FromJSON(yyjson_val *obj) {
		auto error = TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return *this;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		auto type_val = yyjson_obj_get(obj, "type");
		if (!type_val) {
		return "ListType required property 'type' is missing");
		}
		result.type = yyjson_get_str(type_val);

		auto element_id_val = yyjson_obj_get(obj, "element_id");
		if (!element_id_val) {
		return "ListType required property 'element_id' is missing");
		}
		result.element_id = yyjson_get_sint(element_id_val);

		auto element_val = yyjson_obj_get(obj, "element");
		if (!element_val) {
		return "ListType required property 'element' is missing");
		}
		result.element = Type::FromJSON(element_val);

		auto element_required_val = yyjson_obj_get(obj, "element_required");
		if (!element_required_val) {
		return "ListType required property 'element_required' is missing");
		}
		result.element_required = yyjson_get_bool(element_required_val);

		return string();
	}

public:
public:
	string type;
	int64_t element_id;
	Type element;
	bool element_required;
};

} // namespace rest_api_objects
} // namespace duckdb
