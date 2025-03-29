#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class DeleteFile {
public:
	static DeleteFile FromJSON(yyjson_val *obj) {
		DeleteFile result;
		auto discriminator_val = yyjson_obj_get(obj, "content");
		if (discriminator_val && strcmp(yyjson_get_str(discriminator_val), "position-deletes") == 0) {
			result.position_delete_file = PositionDeleteFile::FromJSON(obj);
			result.has_position_delete_file = true;
		} else if (discriminator_val && strcmp(yyjson_get_str(discriminator_val), "equality-deletes") == 0) {
			result.equality_delete_file = EqualityDeleteFile::FromJSON(obj);
			result.has_equality_delete_file = true;
		} else {
			throw IOException("DeleteFile failed to parse, none of the accepted schemas found");
		}
		return result;
	}

public:
	PositionDeleteFile position_delete_file;
	bool has_position_delete_file = false;
	EqualityDeleteFile equality_delete_file;
	bool has_equality_delete_file = false;
};
} // namespace rest_api_objects
} // namespace duckdb
