#pragma once

#include "yyjson.hpp"
#include <string>
#include <vector>
#include <unordered_map>
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/expression_type.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class FalseExpression {
public:
	static FalseExpression FromJSON(yyjson_val *obj) {
		FalseExpression result;

		auto type_val = yyjson_obj_get(obj, "type");
		if (type_val) {
			result.type = ExpressionType::FromJSON(type_val);
		}
		else {
			throw IOException("FalseExpression required property 'type' is missing");
		}

		return result;
	}

public:
	ExpressionType type;
};
} // namespace rest_api_objects
} // namespace duckdb