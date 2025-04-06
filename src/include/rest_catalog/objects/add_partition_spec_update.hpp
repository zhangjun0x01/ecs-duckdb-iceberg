
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/base_update.hpp"
#include "rest_catalog/objects/partition_spec.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class AddPartitionSpecUpdate {
public:
	AddPartitionSpecUpdate() {
	}

public:
	static AddPartitionSpecUpdate FromJSON(yyjson_val *obj) {
		AddPartitionSpecUpdate res;
		auto error = res.TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return res;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		error = base_update.TryFromJSON(obj);
		if (!error.empty()) {
			return error;
		}

		auto spec_val = yyjson_obj_get(obj, "spec");
		if (!spec_val) {
			return "AddPartitionSpecUpdate required property 'spec' is missing";
		} else {
			error = spec.TryFromJSON(spec_val);
			if (!error.empty()) {
				return error;
			}
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
	PartitionSpec spec;
};

} // namespace rest_api_objects
} // namespace duckdb
