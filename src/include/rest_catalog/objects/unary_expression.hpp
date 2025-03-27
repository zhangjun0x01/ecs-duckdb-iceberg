#pragma once

#include "yyjson.hpp"
#include <string>
#include <vector>
#include <unordered_map>
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/term.hpp"
#include "rest_catalog/objects/expression_type.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class UnaryExpression {
public:
	static UnaryExpression FromJSON(yyjson_val *obj) {
		UnaryExpression result;
		auto type_val = yyjson_obj_get(obj, "type");
		if (type_val) {
			result.type = ExpressionType::FromJSON(type_val);
		}
		auto term_val = yyjson_obj_get(obj, "term");
		if (term_val) {
			result.term = Term::FromJSON(term_val);
		}
		auto value_val = yyjson_obj_get(obj, "value");
		if (value_val) {
			result.value = value_val;
		}
		return result;
	}
public:
	ExpressionType type;
	Term term;
	yyjson_val * value;
};

} // namespace rest_api_objects
} // namespace duckdb