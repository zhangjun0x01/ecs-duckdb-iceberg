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
	static StatisticsFile FromJSON(yyjson_val *obj) {
		StatisticsFile result;

		auto blob_metadata_val = yyjson_obj_get(obj, "blob-metadata");
		if (blob_metadata_val) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(blob_metadata_val, idx, max, val) {
				result.blob_metadata.push_back(BlobMetadata::FromJSON(val));
			}
		} else {
			throw IOException("StatisticsFile required property 'blob-metadata' is missing");
		}

		auto file_footer_size_in_bytes_val = yyjson_obj_get(obj, "file-footer-size-in-bytes");
		if (file_footer_size_in_bytes_val) {
			result.file_footer_size_in_bytes = yyjson_get_sint(file_footer_size_in_bytes_val);
		} else {
			throw IOException("StatisticsFile required property 'file-footer-size-in-bytes' is missing");
		}

		auto file_size_in_bytes_val = yyjson_obj_get(obj, "file-size-in-bytes");
		if (file_size_in_bytes_val) {
			result.file_size_in_bytes = yyjson_get_sint(file_size_in_bytes_val);
		} else {
			throw IOException("StatisticsFile required property 'file-size-in-bytes' is missing");
		}

		auto snapshot_id_val = yyjson_obj_get(obj, "snapshot-id");
		if (snapshot_id_val) {
			result.snapshot_id = yyjson_get_sint(snapshot_id_val);
		} else {
			throw IOException("StatisticsFile required property 'snapshot-id' is missing");
		}

		auto statistics_path_val = yyjson_obj_get(obj, "statistics-path");
		if (statistics_path_val) {
			result.statistics_path = yyjson_get_str(statistics_path_val);
		} else {
			throw IOException("StatisticsFile required property 'statistics-path' is missing");
		}

		return result;
	}

public:
	vector<BlobMetadata> blob_metadata;
	int64_t file_footer_size_in_bytes;
	int64_t file_size_in_bytes;
	int64_t snapshot_id;
	string statistics_path;
};
} // namespace rest_api_objects
} // namespace duckdb
