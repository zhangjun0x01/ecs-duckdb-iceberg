#pragma once

#include "yyjson.hpp"
#include <string>
#include <vector>
#include <unordered_map>
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/base_update.hpp"
#include "rest_catalog/objects/partition_spec.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class AddPartitionSpecUpdate {
public:
	static AddPartitionSpecUpdate FromJSON(yyjson_val *obj) {
		AddPartitionSpecUpdate result;
		auto action_val = yyjson_obj_get(obj, "action");
		if (action_val) {
			result.action = yyjson_get_str(action_val);
		}
		auto spec_val = yyjson_obj_get(obj, "spec");
		if (spec_val) {
			result.spec = PartitionSpec::FromJSON(spec_val);
		}
		return result;
	}
public:
	string action;
	PartitionSpec spec;
};

} // namespace rest_api_objects
} // namespace duckdb