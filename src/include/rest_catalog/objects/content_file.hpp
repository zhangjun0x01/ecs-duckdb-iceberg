#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/binary_type_value.hpp"
#include "rest_catalog/objects/file_format.hpp"
#include "rest_catalog/objects/primitive_type_value.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class ContentFile {
public:
	static ContentFile FromJSON(yyjson_val *obj) {
		ContentFile result;

		auto content_val = yyjson_obj_get(obj, "content");
		if (content_val) {
			result.content = yyjson_get_str(content_val);
		}
		else {
			throw IOException("ContentFile required property 'content' is missing");
		}

		auto file_path_val = yyjson_obj_get(obj, "file-path");
		if (file_path_val) {
			result.file_path = yyjson_get_str(file_path_val);
		}
		else {
			throw IOException("ContentFile required property 'file-path' is missing");
		}

		auto file_format_val = yyjson_obj_get(obj, "file-format");
		if (file_format_val) {
			result.file_format = FileFormat::FromJSON(file_format_val);
		}
		else {
			throw IOException("ContentFile required property 'file-format' is missing");
		}

		auto spec_id_val = yyjson_obj_get(obj, "spec-id");
		if (spec_id_val) {
			result.spec_id = yyjson_get_sint(spec_id_val);
		}
		else {
			throw IOException("ContentFile required property 'spec-id' is missing");
		}

		auto partition_val = yyjson_obj_get(obj, "partition");
		if (partition_val) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(partition_val, idx, max, val) {
				result.partition.push_back(PrimitiveTypeValue::FromJSON(val));
			}
		}
		else {
			throw IOException("ContentFile required property 'partition' is missing");
		}

		auto file_size_in_bytes_val = yyjson_obj_get(obj, "file-size-in-bytes");
		if (file_size_in_bytes_val) {
			result.file_size_in_bytes = yyjson_get_sint(file_size_in_bytes_val);
		}
		else {
			throw IOException("ContentFile required property 'file-size-in-bytes' is missing");
		}

		auto record_count_val = yyjson_obj_get(obj, "record-count");
		if (record_count_val) {
			result.record_count = yyjson_get_sint(record_count_val);
		}
		else {
			throw IOException("ContentFile required property 'record-count' is missing");
		}

		auto key_metadata_val = yyjson_obj_get(obj, "key-metadata");
		if (key_metadata_val) {
			result.key_metadata = BinaryTypeValue::FromJSON(key_metadata_val);
		}

		auto split_offsets_val = yyjson_obj_get(obj, "split-offsets");
		if (split_offsets_val) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(split_offsets_val, idx, max, val) {
				result.split_offsets.push_back(yyjson_get_sint(val));
			}
		}

		auto sort_order_id_val = yyjson_obj_get(obj, "sort-order-id");
		if (sort_order_id_val) {
			result.sort_order_id = yyjson_get_sint(sort_order_id_val);
		}

		return result;
	}

public:
	string content;
	string file_path;
	FileFormat file_format;
	int64_t spec_id;
	vector<PrimitiveTypeValue> partition;
	int64_t file_size_in_bytes;
	int64_t record_count;
	BinaryTypeValue key_metadata;
	vector<int64_t> split_offsets;
	int64_t sort_order_id;
};
} // namespace rest_api_objects
} // namespace duckdb