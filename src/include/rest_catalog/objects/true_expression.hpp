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
	static TrueExpression FromJSON(yyjson_val *obj) {
		TrueExpression result;

		auto type_val = yyjson_obj_get(obj, "type");
		if (type_val) {
			result.type = ExpressionType::FromJSON(type_val);
		} else {
			throw IOException("TrueExpression required property 'type' is missing");
		}

		return result;
	}

public:
	ExpressionType type;
};
} // namespace rest_api_objects
} // namespace duckdb
