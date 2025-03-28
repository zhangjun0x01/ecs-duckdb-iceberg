#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/expression.hpp"
#include "rest_catalog/objects/expression_type.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class NotExpression {
public:
	static NotExpression FromJSON(yyjson_val *obj) {
		NotExpression result;

		auto child_val = yyjson_obj_get(obj, "child");
		if (child_val) {
			result.child = Expression::FromJSON(child_val);
		}
		else {
			throw IOException("NotExpression required property 'child' is missing");
		}

		auto type_val = yyjson_obj_get(obj, "type");
		if (type_val) {
			result.type = ExpressionType::FromJSON(type_val);
		}
		else {
			throw IOException("NotExpression required property 'type' is missing");
		}

		return result;
	}

public:
	Expression child;
	ExpressionType type;
};
} // namespace rest_api_objects
} // namespace duckdb