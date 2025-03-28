#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/base_update.hpp"
#include "rest_catalog/objects/partition_statistics_file.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class SetPartitionStatisticsUpdate {
public:
	static SetPartitionStatisticsUpdate FromJSON(yyjson_val *obj) {
		SetPartitionStatisticsUpdate result;

		// Parse BaseUpdate fields
		result.base_update = BaseUpdate::FromJSON(obj);

		auto action_val = yyjson_obj_get(obj, "action");
		if (action_val) {
			result.action = yyjson_get_str(action_val);
		}

		auto partition_statistics_val = yyjson_obj_get(obj, "partition-statistics");
		if (partition_statistics_val) {
			result.partition_statistics = PartitionStatisticsFile::FromJSON(partition_statistics_val);
		}
		else {
			throw IOException("SetPartitionStatisticsUpdate required property 'partition-statistics' is missing");
		}

		return result;
	}

public:
	BaseUpdate base_update;
	string action;
	PartitionStatisticsFile partition_statistics;
};
} // namespace rest_api_objects
} // namespace duckdb