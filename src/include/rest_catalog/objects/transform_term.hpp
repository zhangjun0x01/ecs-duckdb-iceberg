#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/reference.hpp"
#include "rest_catalog/objects/transform.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class TransformTerm {
public:
	static TransformTerm FromJSON(yyjson_val *obj) {
		TransformTerm result;

		auto type_val = yyjson_obj_get(obj, "type");
		if (type_val) {
			result.type = yyjson_get_str(type_val);
		}
		else {
			throw IOException("TransformTerm required property 'type' is missing");
		}

		auto transform_val = yyjson_obj_get(obj, "transform");
		if (transform_val) {
			result.transform = Transform::FromJSON(transform_val);
		}
		else {
			throw IOException("TransformTerm required property 'transform' is missing");
		}

		auto term_val = yyjson_obj_get(obj, "term");
		if (term_val) {
			result.term = Reference::FromJSON(term_val);
		}
		else {
			throw IOException("TransformTerm required property 'term' is missing");
		}

		return result;
	}

public:
	string type;
	Transform transform;
	Reference term;
};
} // namespace rest_api_objects
} // namespace duckdb