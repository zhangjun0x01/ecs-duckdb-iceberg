#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class Expression {
public:
	static Expression FromJSON(yyjson_val *obj) {
		Expression result;
		if (yyjson_is_obj(obj)) {
			auto type_val = yyjson_obj_get(obj, "type");
			if (type_val && strcmp(yyjson_get_str(type_val), "true") == 0) {
				result.true_expression = TrueExpression::FromJSON(obj);
				result.has_true_expression = true;
			}
			if (type_val && strcmp(yyjson_get_str(type_val), "false") == 0) {
				result.false_expression = FalseExpression::FromJSON(obj);
				result.has_false_expression = true;
			}
			if (type_val && strcmp(yyjson_get_str(type_val), "not") == 0) {
				result.not_expression = NotExpression::FromJSON(obj);
				result.has_not_expression = true;
			}
		}
		return result;
	}

public:
	TrueExpression true_expression;
	bool has_true_expression = false;
	FalseExpression false_expression;
	bool has_false_expression = false;
	AndOrExpression and_or_expression;
	bool has_and_or_expression = false;
	NotExpression not_expression;
	bool has_not_expression = false;
	SetExpression set_expression;
	bool has_set_expression = false;
	LiteralExpression literal_expression;
	bool has_literal_expression = false;
	UnaryExpression unary_expression;
	bool has_unary_expression = false;
};
} // namespace rest_api_objects
} // namespace duckdb
