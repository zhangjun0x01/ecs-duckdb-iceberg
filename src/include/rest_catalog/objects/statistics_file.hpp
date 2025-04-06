
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/blob_metadata.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class StatisticsFile {
public:
	StatisticsFile::StatisticsFile() {
	}

public:
	static StatisticsFile FromJSON(yyjson_val *obj) {
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
		return "StatisticsFile required property 'snapshot_id' is missing");
		}
		result.snapshot_id = yyjson_get_sint(snapshot_id_val);

		auto statistics_path_val = yyjson_obj_get(obj, "statistics_path");
		if (!statistics_path_val) {
		return "StatisticsFile required property 'statistics_path' is missing");
		}
		result.statistics_path = yyjson_get_str(statistics_path_val);

		auto file_size_in_bytes_val = yyjson_obj_get(obj, "file_size_in_bytes");
		if (!file_size_in_bytes_val) {
		return "StatisticsFile required property 'file_size_in_bytes' is missing");
		}
		result.file_size_in_bytes = yyjson_get_sint(file_size_in_bytes_val);

		auto file_footer_size_in_bytes_val = yyjson_obj_get(obj, "file_footer_size_in_bytes");
		if (!file_footer_size_in_bytes_val) {
		return "StatisticsFile required property 'file_footer_size_in_bytes' is missing");
		}
		result.file_footer_size_in_bytes = yyjson_get_sint(file_footer_size_in_bytes_val);

		auto blob_metadata_val = yyjson_obj_get(obj, "blob_metadata");
		if (!blob_metadata_val) {
		return "StatisticsFile required property 'blob_metadata' is missing");
		}
		size_t idx, max;
		yyjson_val *val;
		yyjson_arr_foreach(blob_metadata_val, idx, max, val) {
			result.blob_metadata.push_back(BlobMetadata::FromJSON(val));
		}

		return string();
	}

public:
public:
};

} // namespace rest_api_objects
} // namespace duckdb
