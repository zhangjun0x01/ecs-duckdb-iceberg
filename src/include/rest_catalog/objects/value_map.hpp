#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/primitive_type_value.hpp"
#include "rest_catalog/objects/integer_type_value.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class ValueMap {
public:
	static ValueMap FromJSON(yyjson_val *obj) {
		ValueMap result;

		auto keys_val = yyjson_obj_get(obj, "keys");
		if (keys_val) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(keys_val, idx, max, val) {
				result.keys.push_back(IntegerTypeValue::FromJSON(val));
			}
		}

		auto values_val = yyjson_obj_get(obj, "values");
		if (values_val) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(values_val, idx, max, val) {
				result.values.push_back(PrimitiveTypeValue::FromJSON(val));
			}
		}

		return result;
	}

public:
	vector<IntegerTypeValue> keys;
	vector<PrimitiveTypeValue> values;
};
} // namespace rest_api_objects
} // namespace duckdb