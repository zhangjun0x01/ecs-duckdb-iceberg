#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class Term {
public:
	static Term FromJSON(yyjson_val *obj) {
		Term result;
		if (yyjson_is_str(obj)) {
			result.value_string = yyjson_get_str(obj);
			result.has_string = true;
		}
		if (yyjson_is_obj(obj)) {
			auto type_val = yyjson_obj_get(obj, "type");
			if (type_val && strcmp(yyjson_get_str(type_val), "transform") == 0) {
				result.transform_term = TransformTerm::FromJSON(obj);
				result.has_transform_term = true;
			}
		}
		return result;
	}

public:
	string value_string;
	bool has_string = false;
	TransformTerm transform_term;
	bool has_transform_term = false;
};
} // namespace rest_api_objects
} // namespace duckdb
