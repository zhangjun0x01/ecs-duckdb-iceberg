
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

class EqualityDeleteFile {
public:
	EqualityDeleteFile::EqualityDeleteFile() {
	}

public:
	static EqualityDeleteFile FromJSON(yyjson_val *obj) {
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
		return "EqualityDeleteFile required property 'content' is missing");
		}
		result.content = yyjson_get_str(content_val);

		auto equality_ids_val = yyjson_obj_get(obj, "equality_ids");
		if (equality_ids_val) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(equality_ids_val, idx, max, val) {
				result.equality_ids.push_back(yyjson_get_sint(val));
			};
		}
		return string();
	}

public:
	ContentFile content_file;

public:
};

} // namespace rest_api_objects
} // namespace duckdb
