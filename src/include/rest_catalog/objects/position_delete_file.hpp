#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/content_file.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class PositionDeleteFile {
public:
	static PositionDeleteFile FromJSON(yyjson_val *obj) {
		PositionDeleteFile result;

		// Parse ContentFile fields
		result.content_file = ContentFile::FromJSON(obj);

		auto content_val = yyjson_obj_get(obj, "content");
		if (content_val) {
			result.content = yyjson_get_str(content_val);
		} else {
			throw IOException("PositionDeleteFile required property 'content' is missing");
		}

		auto content_offset_val = yyjson_obj_get(obj, "content-offset");
		if (content_offset_val) {
			result.content_offset = yyjson_get_sint(content_offset_val);
		}

		auto content_size_in_bytes_val = yyjson_obj_get(obj, "content-size-in-bytes");
		if (content_size_in_bytes_val) {
			result.content_size_in_bytes = yyjson_get_sint(content_size_in_bytes_val);
		}

		return result;
	}

public:
	ContentFile content_file;
	string content;
	int64_t content_offset;
	int64_t content_size_in_bytes;
};
} // namespace rest_api_objects
} // namespace duckdb
