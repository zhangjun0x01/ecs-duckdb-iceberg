
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/expression_type.hpp"
#include "rest_catalog/objects/term.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class LiteralExpression {
public:
	LiteralExpression::LiteralExpression() {
	}

public:
	static LiteralExpression FromJSON(yyjson_val *obj) {
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
		return "LiteralExpression required property 'type' is missing");
		}
		error = expression_type.TryFromJSON(type_val);
		if (!error.empty()) {
			return error;
		}

		auto term_val = yyjson_obj_get(obj, "term");
		if (!term_val) {
		return "LiteralExpression required property 'term' is missing");
		}
		error = term.TryFromJSON(term_val);
		if (!error.empty()) {
			return error;
		}

		auto value_val = yyjson_obj_get(obj, "value");
		if (!value_val) {
		return "LiteralExpression required property 'value' is missing");
		}
		value = value_val;

		return string();
	}

public:
public:
	Term term;
	ExpressionType type;
	yyjson_val *value;
};

} // namespace rest_api_objects
} // namespace duckdb
