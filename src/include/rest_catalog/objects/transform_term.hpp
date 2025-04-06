
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
	TransformTerm::TransformTerm() {
	}

public:
	static TransformTerm FromJSON(yyjson_val *obj) {
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
		return "TransformTerm required property 'type' is missing");
		}
		result.type = yyjson_get_str(type_val);

		auto transform_val = yyjson_obj_get(obj, "transform");
		if (!transform_val) {
		return "TransformTerm required property 'transform' is missing");
		}
		result.transform = Transform::FromJSON(transform_val);

		auto term_val = yyjson_obj_get(obj, "term");
		if (!term_val) {
		return "TransformTerm required property 'term' is missing");
		}
		result.term = Reference::FromJSON(term_val);

		return string();
	}

public:
public:
	Reference term;
	Transform transform;
	string type;
};

} // namespace rest_api_objects
} // namespace duckdb
