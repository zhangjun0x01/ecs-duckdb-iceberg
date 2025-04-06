
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

class SetExpression {
public:
	SetExpression::SetExpression() {
	}

public:
	static SetExpression FromJSON(yyjson_val *obj) {
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
		return "SetExpression required property 'type' is missing");
		}
		result.type = ExpressionType::FromJSON(type_val);

		auto term_val = yyjson_obj_get(obj, "term");
		if (!term_val) {
		return "SetExpression required property 'term' is missing");
		}
		result.term = Term::FromJSON(term_val);

		auto values_val = yyjson_obj_get(obj, "values");
		if (!values_val) {
		return "SetExpression required property 'values' is missing");
		}
		size_t idx, max;
		yyjson_val *val;
		yyjson_arr_foreach(values_val, idx, max, val) {
			result.values.push_back(val);
		}

		return string();
	}

public:
public:
};

} // namespace rest_api_objects
} // namespace duckdb
