
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/base_update.hpp"
#include "rest_catalog/objects/partition_spec.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class AddPartitionSpecUpdate {
public:
	AddPartitionSpecUpdate();
	AddPartitionSpecUpdate(const AddPartitionSpecUpdate &) = delete;
	AddPartitionSpecUpdate &operator=(const AddPartitionSpecUpdate &) = delete;
	AddPartitionSpecUpdate(AddPartitionSpecUpdate &&) = default;
	AddPartitionSpecUpdate &operator=(AddPartitionSpecUpdate &&) = default;

public:
	static AddPartitionSpecUpdate FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	BaseUpdate base_update;
	PartitionSpec spec;
	string action;
	bool has_action = false;
};

} // namespace rest_api_objects
} // namespace duckdb
