#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class CatalogConfig {
public:
	static CatalogConfig FromJSON(yyjson_val *obj) {
		CatalogConfig result;

		auto defaults_val = yyjson_obj_get(obj, "defaults");
		if (defaults_val) {
			result.defaults = parse_object_of_strings(defaults_val);
		} else {
			throw IOException("CatalogConfig required property 'defaults' is missing");
		}

		auto endpoints_val = yyjson_obj_get(obj, "endpoints");
		if (endpoints_val) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(endpoints_val, idx, max, val) {
				result.endpoints.push_back(yyjson_get_str(val));
			}
		}

		auto overrides_val = yyjson_obj_get(obj, "overrides");
		if (overrides_val) {
			result.overrides = parse_object_of_strings(overrides_val);
		} else {
			throw IOException("CatalogConfig required property 'overrides' is missing");
		}

		return result;
	}

public:
	case_insensitive_map_t<string> defaults;
	vector<string> endpoints;
	case_insensitive_map_t<string> overrides;
};
} // namespace rest_api_objects
} // namespace duckdb
