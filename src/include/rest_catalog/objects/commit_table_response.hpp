#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/table_metadata.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class CommitTableResponse {
public:
	static CommitTableResponse FromJSON(yyjson_val *obj) {
		CommitTableResponse result;

		auto metadata_location_val = yyjson_obj_get(obj, "metadata-location");
		if (metadata_location_val) {
			result.metadata_location = yyjson_get_str(metadata_location_val);
		}
		else {
			throw IOException("CommitTableResponse required property 'metadata-location' is missing");
		}

		auto metadata_val = yyjson_obj_get(obj, "metadata");
		if (metadata_val) {
			result.metadata = TableMetadata::FromJSON(metadata_val);
		}
		else {
			throw IOException("CommitTableResponse required property 'metadata' is missing");
		}

		return result;
	}

public:
	string metadata_location;
	TableMetadata metadata;
};
} // namespace rest_api_objects
} // namespace duckdb