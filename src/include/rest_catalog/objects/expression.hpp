
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/and_or_expression.hpp"
#include "rest_catalog/objects/false_expression.hpp"
#include "rest_catalog/objects/literal_expression.hpp"
#include "rest_catalog/objects/not_expression.hpp"
#include "rest_catalog/objects/set_expression.hpp"
#include "rest_catalog/objects/true_expression.hpp"
#include "rest_catalog/objects/unary_expression.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class Expression {
public:
	Expression::Expression() {
	}

public:
	static Expression FromJSON(yyjson_val *obj) {
		auto error = TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return *this;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;
		do {
			error = base_true_expression.TryFromJSON(obj);
			if (error.empty()) {
				has_true_expression = true;
				break;
			}
			error = base_false_expression.TryFromJSON(obj);
			if (error.empty()) {
				has_false_expression = true;
				break;
			}
			error = base_and_or_expression.TryFromJSON(obj);
			if (error.empty()) {
				has_and_or_expression = true;
				break;
			}
			error = base_not_expression.TryFromJSON(obj);
			if (error.empty()) {
				has_not_expression = true;
				break;
			}
			error = base_set_expression.TryFromJSON(obj);
			if (error.empty()) {
				has_set_expression = true;
				break;
			}
			error = base_literal_expression.TryFromJSON(obj);
			if (error.empty()) {
				has_literal_expression = true;
				break;
			}
			error = base_unary_expression.TryFromJSON(obj);
			if (error.empty()) {
				has_unary_expression = true;
				break;
			}
			return "Expression failed to parse, none of the oneOf candidates matched";
		} while (false);

		return string();
	}

public:
	UnaryExpression unary_expression;
	FalseExpression false_expression;
	NotExpression not_expression;
	SetExpression set_expression;
	AndOrExpression and_or_expression;
	TrueExpression true_expression;
	LiteralExpression literal_expression;

public:
};

} // namespace rest_api_objects
} // namespace duckdb
