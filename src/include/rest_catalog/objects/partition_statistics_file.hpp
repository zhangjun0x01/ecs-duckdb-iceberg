
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class PartitionStatisticsFile {
public:
	PartitionStatisticsFile::PartitionStatisticsFile() {
	}

public:
	static PartitionStatisticsFile FromJSON(yyjson_val *obj) {
		auto error = TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return *this;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		auto snapshot_id_val = yyjson_obj_get(obj, "snapshot_id");
		if (!snapshot_id_val) {
		return "PartitionStatisticsFile required property 'snapshot_id' is missing");
		}
		snapshot_id = yyjson_get_sint(snapshot_id_val);

		auto statistics_path_val = yyjson_obj_get(obj, "statistics_path");
		if (!statistics_path_val) {
		return "PartitionStatisticsFile required property 'statistics_path' is missing");
		}
		statistics_path = yyjson_get_str(statistics_path_val);

		auto file_size_in_bytes_val = yyjson_obj_get(obj, "file_size_in_bytes");
		if (!file_size_in_bytes_val) {
		return "PartitionStatisticsFile required property 'file_size_in_bytes' is missing");
		}
		file_size_in_bytes = yyjson_get_sint(file_size_in_bytes_val);

		return string();
	}

public:
public:
	int64_t file_size_in_bytes;
	int64_t snapshot_id;
	string statistics_path;
};

} // namespace rest_api_objects
} // namespace duckdb
