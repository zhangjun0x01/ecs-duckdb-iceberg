
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/integer_type_value.hpp"
#include "rest_catalog/objects/long_type_value.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class CountMap {
public:
	CountMap::CountMap() {
	}

public:
	static CountMap FromJSON(yyjson_val *obj) {
		auto error = TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return *this;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

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
				result.values.push_back(LongTypeValue::FromJSON(val));
			}
		}
		return string();
	}

public:
public:
	vector<IntegerTypeValue> keys;
	vector<LongTypeValue> values;
};

} // namespace rest_api_objects
} // namespace duckdb
