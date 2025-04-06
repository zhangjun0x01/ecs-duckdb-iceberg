
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
	RenameTableRequest() {
	}

public:
	static RenameTableRequest FromJSON(yyjson_val *obj) {
		RenameTableRequest res;
		auto error = res.TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return res;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		auto source_val = yyjson_obj_get(obj, "source");
		if (!source_val) {
			return "RenameTableRequest required property 'source' is missing";
		} else {
			error = source.TryFromJSON(source_val);
			if (!error.empty()) {
				return error;
			}
		}

		auto destination_val = yyjson_obj_get(obj, "destination");
		if (!destination_val) {
			return "RenameTableRequest required property 'destination' is missing";
		} else {
			error = destination.TryFromJSON(destination_val);
			if (!error.empty()) {
				return error;
			}
		}

		return string();
	}

public:
public:
	TableIdentifier destination;
	TableIdentifier source;
};

} // namespace rest_api_objects
} // namespace duckdb
