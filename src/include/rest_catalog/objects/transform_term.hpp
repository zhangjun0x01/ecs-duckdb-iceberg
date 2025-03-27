#pragma once

#include "yyjson.hpp"
#include <string>
#include <vector>
#include <unordered_map>
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/transform.hpp"
#include "rest_catalog/objects/reference.hpp"

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
		auto transform_val = yyjson_obj_get(obj, "transform");
		if (transform_val) {
			result.transform = Transform::FromJSON(transform_val);
		}
		auto term_val = yyjson_obj_get(obj, "term");
		if (term_val) {
			result.term = Reference::FromJSON(term_val);
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