#pragma once

#include "yyjson.hpp"
#include <string>
#include <vector>
#include <unordered_map>
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class PartitionStatisticsFile {
public:
	static PartitionStatisticsFile FromJSON(yyjson_val *obj) {
		PartitionStatisticsFile result;
		auto snapshot_id_val = yyjson_obj_get(obj, "snapshot-id");
		if (snapshot_id_val) {
			result.snapshot_id = yyjson_get_sint(snapshot_id_val);
		}
		auto statistics_path_val = yyjson_obj_get(obj, "statistics-path");
		if (statistics_path_val) {
			result.statistics_path = yyjson_get_str(statistics_path_val);
		}
		auto file_size_in_bytes_val = yyjson_obj_get(obj, "file-size-in-bytes");
		if (file_size_in_bytes_val) {
			result.file_size_in_bytes = yyjson_get_sint(file_size_in_bytes_val);
		}
		return result;
	}
public:
	int64_t snapshot_id;
	string statistics_path;
	int64_t file_size_in_bytes;
};

} // namespace rest_api_objects
} // namespace duckdb