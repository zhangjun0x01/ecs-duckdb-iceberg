
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/expression_type.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class TrueExpression {
public:
	TrueExpression::TrueExpression() {
	}

public:
	static TrueExpression FromJSON(yyjson_val *obj) {
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
		return "TrueExpression required property 'type' is missing");
		}
		error = type.TryFromJSON(type_val);
		if (!error.empty()) {
			return error;
		}

		return string();
	}

public:
public:
	ExpressionType type;
};

} // namespace rest_api_objects
} // namespace duckdb
