
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class UpdateNamespacePropertiesRequest {
public:
	UpdateNamespacePropertiesRequest::UpdateNamespacePropertiesRequest() {
	}

public:
	static UpdateNamespacePropertiesRequest FromJSON(yyjson_val *obj) {
		auto error = TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return *this;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		auto removals_val = yyjson_obj_get(obj, "removals");
		if (removals_val) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(removals_val, idx, max, val) {

				auto tmp = yyjson_get_str(val);
				removals.push_back(tmp);
			}
		}

		auto updates_val = yyjson_obj_get(obj, "updates");
		if (updates_val) {
			updates = parse_object_of_strings(updates_val);
		}
		return string();
	}

public:
public:
	vector<string> removals;
	case_insensitive_map_t<string> updates;
};

} // namespace rest_api_objects
} // namespace duckdb
