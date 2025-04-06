
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

class FalseExpression {
public:
	FalseExpression() {
	}

public:
	static FalseExpression FromJSON(yyjson_val *obj) {
		FalseExpression res;
		auto error = res.TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return res;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;
		auto type_val = yyjson_obj_get(obj, "type");
		if (!type_val) {
			return "FalseExpression required property 'type' is missing";
		} else {
			error = type.TryFromJSON(type_val);
			if (!error.empty()) {
				return error;
			}
		}
		return string();
	}

public:
	ExpressionType type;
};

} // namespace rest_api_objects
} // namespace duckdb
