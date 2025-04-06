
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/reference.hpp"
#include "rest_catalog/objects/transform.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class TransformTerm {
public:
	TransformTerm() {
	}

public:
	static TransformTerm FromJSON(yyjson_val *obj) {
		TransformTerm res;
		auto error = res.TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return res;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;
		auto type_val = yyjson_obj_get(obj, "type");
		if (!type_val) {
			return "TransformTerm required property 'type' is missing";
		} else {
			type = yyjson_get_str(type_val);
		}
		auto transform_val = yyjson_obj_get(obj, "transform");
		if (!transform_val) {
			return "TransformTerm required property 'transform' is missing";
		} else {
			error = transform.TryFromJSON(transform_val);
			if (!error.empty()) {
				return error;
			}
		}
		auto term_val = yyjson_obj_get(obj, "term");
		if (!term_val) {
			return "TransformTerm required property 'term' is missing";
		} else {
			error = term.TryFromJSON(term_val);
			if (!error.empty()) {
				return error;
			}
		}
		return string();
	}

public:
	string type;
	Transform transform;
	Reference term;
};

} // namespace rest_api_objects
} // namespace duckdb
