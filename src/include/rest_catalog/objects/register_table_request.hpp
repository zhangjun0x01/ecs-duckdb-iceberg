
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class RegisterTableRequest {
public:
	RegisterTableRequest::RegisterTableRequest() {
	}

public:
	static RegisterTableRequest FromJSON(yyjson_val *obj) {
		auto error = TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return *this;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		auto name_val = yyjson_obj_get(obj, "name");
		if (!name_val) {
		return "RegisterTableRequest required property 'name' is missing");
		}
		result.name = yyjson_get_str(name_val);

		auto metadata_location_val = yyjson_obj_get(obj, "metadata_location");
		if (!metadata_location_val) {
		return "RegisterTableRequest required property 'metadata_location' is missing");
		}
		result.metadata_location = yyjson_get_str(metadata_location_val);

		auto overwrite_val = yyjson_obj_get(obj, "overwrite");
		if (overwrite_val) {
			result.overwrite = yyjson_get_bool(overwrite_val);
			;
		}
		return string();
	}

public:
public:
};

} // namespace rest_api_objects
} // namespace duckdb
