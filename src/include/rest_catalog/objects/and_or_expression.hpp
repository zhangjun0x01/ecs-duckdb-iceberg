#pragma once

#include "yyjson.hpp"
#include <string>
#include <vector>
#include <unordered_map>
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/expression.hpp"
#include "rest_catalog/objects/expression_type.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class AndOrExpression {
public:
	static AndOrExpression FromJSON(yyjson_val *obj) {
		AndOrExpression result;
		auto type_val = yyjson_obj_get(obj, "type");
		if (type_val) {
			result.type = ExpressionType::FromJSON(type_val);
		}
		auto left_val = yyjson_obj_get(obj, "left");
		if (left_val) {
			result.left = Expression::FromJSON(left_val);
		}
		auto right_val = yyjson_obj_get(obj, "right");
		if (right_val) {
			result.right = Expression::FromJSON(right_val);
		}
		return result;
	}
public:
	ExpressionType type;
	Expression left;
	Expression right;
};

} // namespace rest_api_objects
} // namespace duckdb