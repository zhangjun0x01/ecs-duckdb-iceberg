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
	static LiteralExpression FromJSON(yyjson_val *obj) {
		LiteralExpression result;

		auto term_val = yyjson_obj_get(obj, "term");
		if (term_val) {
			result.term = Term::FromJSON(term_val);
		}
		else {
			throw IOException("LiteralExpression required property 'term' is missing");
		}

		auto type_val = yyjson_obj_get(obj, "type");
		if (type_val) {
			result.type = ExpressionType::FromJSON(type_val);
		}
		else {
			throw IOException("LiteralExpression required property 'type' is missing");
		}

		auto value_val = yyjson_obj_get(obj, "value");
		if (value_val) {
			result.value = value_val;
		}
		else {
			throw IOException("LiteralExpression required property 'value' is missing");
		}

		return result;
	}

public:
	Term term;
	ExpressionType type;
	yyjson_val * value;
};
} // namespace rest_api_objects
} // namespace duckdb