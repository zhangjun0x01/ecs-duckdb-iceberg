
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
	PositionDeleteFile::PositionDeleteFile() {
	}

public:
	static PositionDeleteFile FromJSON(yyjson_val *obj) {
		auto error = TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return *this;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		error = base_content_file.TryFromJSON(obj);
		if (!error.empty()) {
			return error;
		}

		auto content_val = yyjson_obj_get(obj, "content");
		if (!content_val) {
		return "PositionDeleteFile required property 'content' is missing");
		}
		result.content = yyjson_get_str(content_val);

		auto content_offset_val = yyjson_obj_get(obj, "content_offset");
		if (content_offset_val) {
			result.content_offset = yyjson_get_sint(content_offset_val);
			;
		}

		auto content_size_in_bytes_val = yyjson_obj_get(obj, "content_size_in_bytes");
		if (content_size_in_bytes_val) {
			result.content_size_in_bytes = yyjson_get_sint(content_size_in_bytes_val);
			;
		}
		return string();
	}

public:
	ContentFile content_file;

public:
};

} // namespace rest_api_objects
} // namespace duckdb
