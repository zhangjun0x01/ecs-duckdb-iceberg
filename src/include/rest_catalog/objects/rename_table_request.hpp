
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
	RenameTableRequest::RenameTableRequest() {
	}

public:
	static RenameTableRequest FromJSON(yyjson_val *obj) {
		auto error = TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return *this;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		auto source_val = yyjson_obj_get(obj, "source");
		if (!source_val) {
		return "RenameTableRequest required property 'source' is missing");
		}
		result.source = TableIdentifier::FromJSON(source_val);

		auto destination_val = yyjson_obj_get(obj, "destination");
		if (!destination_val) {
		return "RenameTableRequest required property 'destination' is missing");
		}
		result.destination = TableIdentifier::FromJSON(destination_val);

		return string();
	}

public:
public:
	TableIdentifier destination;
	TableIdentifier source;
};

} // namespace rest_api_objects
} // namespace duckdb
