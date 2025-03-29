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
	static SetExpression FromJSON(yyjson_val *obj) {
		SetExpression result;

		auto term_val = yyjson_obj_get(obj, "term");
		if (term_val) {
			result.term = Term::FromJSON(term_val);
		} else {
			throw IOException("SetExpression required property 'term' is missing");
		}

		auto type_val = yyjson_obj_get(obj, "type");
		if (type_val) {
			result.type = ExpressionType::FromJSON(type_val);
		} else {
			throw IOException("SetExpression required property 'type' is missing");
		}

		auto values_val = yyjson_obj_get(obj, "values");
		if (values_val) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(values_val, idx, max, val) {
				result.values.push_back(val);
			}
		} else {
			throw IOException("SetExpression required property 'values' is missing");
		}

		return result;
	}

public:
	Term term;
	ExpressionType type;
	vector<yyjson_val *> values;
};
} // namespace rest_api_objects
} // namespace duckdb
