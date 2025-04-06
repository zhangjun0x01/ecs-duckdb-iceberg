
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
	NotExpression::NotExpression() {
	}

public:
	static NotExpression FromJSON(yyjson_val *obj) {
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
		return "NotExpression required property 'type' is missing");
		}
		result.type = ExpressionType::FromJSON(type_val);

		auto child_val = yyjson_obj_get(obj, "child");
		if (!child_val) {
		return "NotExpression required property 'child' is missing");
		}
		result.child = Expression::FromJSON(child_val);

		return string();
	}

public:
public:
};

} // namespace rest_api_objects
} // namespace duckdb
