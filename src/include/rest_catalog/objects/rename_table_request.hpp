#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/table_identifier.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class RenameTableRequest {
public:
	static RenameTableRequest FromJSON(yyjson_val *obj) {
		RenameTableRequest result;

		auto destination_val = yyjson_obj_get(obj, "destination");
		if (destination_val) {
			result.destination = TableIdentifier::FromJSON(destination_val);
		} else {
			throw IOException("RenameTableRequest required property 'destination' is missing");
		}

		auto source_val = yyjson_obj_get(obj, "source");
		if (source_val) {
			result.source = TableIdentifier::FromJSON(source_val);
		} else {
			throw IOException("RenameTableRequest required property 'source' is missing");
		}

		return result;
	}

public:
	TableIdentifier destination;
	TableIdentifier source;
};
} // namespace rest_api_objects
} // namespace duckdb
