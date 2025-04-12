
#include "rest_catalog/objects/statistics_file.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

StatisticsFile::StatisticsFile() {
}

StatisticsFile StatisticsFile::FromJSON(yyjson_val *obj) {
	StatisticsFile res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string StatisticsFile::TryFromJSON(yyjson_val *obj) {
	string error;
	auto snapshot_id_val = yyjson_obj_get(obj, "snapshot-id");
	if (!snapshot_id_val) {
		return "StatisticsFile required property 'snapshot-id' is missing";
	} else {
		if (yyjson_is_sint(snapshot_id_val)) {
			snapshot_id = yyjson_get_sint(snapshot_id_val);
		} else {
			return "StatisticsFile property 'snapshot_id' is not of type 'integer'";
		}
	}
	auto statistics_path_val = yyjson_obj_get(obj, "statistics-path");
	if (!statistics_path_val) {
		return "StatisticsFile required property 'statistics-path' is missing";
	} else {
		if (yyjson_is_str(statistics_path_val)) {
			statistics_path = yyjson_get_str(statistics_path_val);
		} else {
			return "StatisticsFile property 'statistics_path' is not of type 'string'";
		}
	}
	auto file_size_in_bytes_val = yyjson_obj_get(obj, "file-size-in-bytes");
	if (!file_size_in_bytes_val) {
		return "StatisticsFile required property 'file-size-in-bytes' is missing";
	} else {
		if (yyjson_is_sint(file_size_in_bytes_val)) {
			file_size_in_bytes = yyjson_get_sint(file_size_in_bytes_val);
		} else {
			return "StatisticsFile property 'file_size_in_bytes' is not of type 'integer'";
		}
	}
	auto file_footer_size_in_bytes_val = yyjson_obj_get(obj, "file-footer-size-in-bytes");
	if (!file_footer_size_in_bytes_val) {
		return "StatisticsFile required property 'file-footer-size-in-bytes' is missing";
	} else {
		if (yyjson_is_sint(file_footer_size_in_bytes_val)) {
			file_footer_size_in_bytes = yyjson_get_sint(file_footer_size_in_bytes_val);
		} else {
			return "StatisticsFile property 'file_footer_size_in_bytes' is not of type 'integer'";
		}
	}
	auto blob_metadata_val = yyjson_obj_get(obj, "blob-metadata");
	if (!blob_metadata_val) {
		return "StatisticsFile required property 'blob-metadata' is missing";
	} else {
		size_t idx, max;
		yyjson_val *val;
		yyjson_arr_foreach(blob_metadata_val, idx, max, val) {
			BlobMetadata tmp;
			error = tmp.TryFromJSON(val);
			if (!error.empty()) {
				return error;
			}
			blob_metadata.emplace_back(std::move(tmp));
		}
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
