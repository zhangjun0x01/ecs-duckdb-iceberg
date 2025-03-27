#pragma once

#include "yyjson.hpp"
#include <string>
#include <vector>
#include <unordered_map>
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/table_identifier.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class RenameTableRequest {
public:
	static RenameTableRequest FromJSON(yyjson_val *obj) {
		RenameTableRequest result;
		auto source_val = yyjson_obj_get(obj, "source");
		if (source_val) {
			result.source = TableIdentifier::FromJSON(source_val);
		}
		auto destination_val = yyjson_obj_get(obj, "destination");
		if (destination_val) {
			result.destination = TableIdentifier::FromJSON(destination_val);
		}
		return result;
	}
public:
	TableIdentifier source;
	TableIdentifier destination;
};

} // namespace rest_api_objects
} // namespace duckdb