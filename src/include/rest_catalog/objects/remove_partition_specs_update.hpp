
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/base_update.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class RemovePartitionSpecsUpdate {
public:
	RemovePartitionSpecsUpdate::RemovePartitionSpecsUpdate() {
	}

public:
	static RemovePartitionSpecsUpdate FromJSON(yyjson_val *obj) {
		auto error = TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return *this;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		error = base_update.TryFromJSON(obj);
		if (!error.empty()) {
			return error;
		}

		auto spec_ids_val = yyjson_obj_get(obj, "spec_ids");
		if (!spec_ids_val) {
		return "RemovePartitionSpecsUpdate required property 'spec_ids' is missing");
		}
		size_t idx, max;
		yyjson_val *val;
		yyjson_arr_foreach(spec_ids_val, idx, max, val) {

			auto tmp = yyjson_get_sint(val);
			spec_ids.push_back(tmp);
		}

		auto action_val = yyjson_obj_get(obj, "action");
		if (action_val) {
			action = yyjson_get_str(action_val);
		}
		return string();
	}

public:
	BaseUpdate base_update;

public:
	string action;
	vector<int64_t> spec_ids;
};

} // namespace rest_api_objects
} // namespace duckdb
