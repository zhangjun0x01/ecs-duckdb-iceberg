#pragma once

#include "yyjson.hpp"
#include <string>
#include <vector>
#include <unordered_map>
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/expression_type.hpp"
#include "rest_catalog/objects/expression.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class NotExpression {
public:
	static NotExpression FromJSON(yyjson_val *obj) {
		NotExpression result;
		auto type_val = yyjson_obj_get(obj, "type");
		if (type_val) {
			result.type = ExpressionType::FromJSON(type_val);
		}
		auto child_val = yyjson_obj_get(obj, "child");
		if (child_val) {
			result.child = Expression::FromJSON(child_val);
		}
		return result;
	}
public:
	ExpressionType type;
	Expression child;
};

} // namespace rest_api_objects
} // namespace duckdb