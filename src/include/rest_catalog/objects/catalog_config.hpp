
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
	CatalogConfig() {
	}

public:
	static CatalogConfig FromJSON(yyjson_val *obj) {
		CatalogConfig res;
		auto error = res.TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return res;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;
		auto defaults_val = yyjson_obj_get(obj, "defaults");
		if (!defaults_val) {
			return "CatalogConfig required property 'defaults' is missing";
		} else {
			defaults = parse_object_of_strings(defaults_val);
		}
		auto overrides_val = yyjson_obj_get(obj, "overrides");
		if (!overrides_val) {
			return "CatalogConfig required property 'overrides' is missing";
		} else {
			overrides = parse_object_of_strings(overrides_val);
		}
		auto endpoints_val = yyjson_obj_get(obj, "endpoints");
		if (endpoints_val) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(endpoints_val, idx, max, val) {
				auto tmp = yyjson_get_str(val);
				endpoints.push_back(tmp);
			}
		}
		return string();
	}

public:
	case_insensitive_map_t<string> defaults;
	case_insensitive_map_t<string> overrides;
	vector<string> endpoints;
};

} // namespace rest_api_objects
} // namespace duckdb
