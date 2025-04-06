
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class UpdateNamespacePropertiesResponse {
public:
	UpdateNamespacePropertiesResponse() {
	}

public:
	static UpdateNamespacePropertiesResponse FromJSON(yyjson_val *obj) {
		UpdateNamespacePropertiesResponse res;
		auto error = res.TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return res;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		auto updated_val = yyjson_obj_get(obj, "updated");
		if (!updated_val) {
			return "UpdateNamespacePropertiesResponse required property 'updated' is missing";
		} else {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(updated_val, idx, max, val) {

				auto tmp = yyjson_get_str(val);
				updated.push_back(tmp);
			}
		}

		auto removed_val = yyjson_obj_get(obj, "removed");
		if (!removed_val) {
			return "UpdateNamespacePropertiesResponse required property 'removed' is missing";
		} else {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(removed_val, idx, max, val) {

				auto tmp = yyjson_get_str(val);
				removed.push_back(tmp);
			}
		}

		auto missing_val = yyjson_obj_get(obj, "missing");
		if (missing_val) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(missing_val, idx, max, val) {

				auto tmp = yyjson_get_str(val);
				missing.push_back(tmp);
			}
		}

		return string();
	}

public:
public:
	vector<string> missing;
	vector<string> removed;
	vector<string> updated;
};

} // namespace rest_api_objects
} // namespace duckdb
