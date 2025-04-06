
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/schema.hpp"
#include "rest_catalog/objects/view_version.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class CreateViewRequest {
public:
	CreateViewRequest::CreateViewRequest() {
	}

public:
	static CreateViewRequest FromJSON(yyjson_val *obj) {
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
		return "CreateViewRequest required property 'name' is missing");
		}
		result.name = yyjson_get_str(name_val);

		auto schema_val = yyjson_obj_get(obj, "schema");
		if (!schema_val) {
		return "CreateViewRequest required property 'schema' is missing");
		}
		result.schema = Schema::FromJSON(schema_val);

		auto view_version_val = yyjson_obj_get(obj, "view_version");
		if (!view_version_val) {
		return "CreateViewRequest required property 'view_version' is missing");
		}
		result.view_version = ViewVersion::FromJSON(view_version_val);

		auto properties_val = yyjson_obj_get(obj, "properties");
		if (!properties_val) {
		return "CreateViewRequest required property 'properties' is missing");
		}
		result.properties = parse_object_of_strings(properties_val);

		auto location_val = yyjson_obj_get(obj, "location");
		if (location_val) {
			result.location = yyjson_get_str(location_val);
		}
		return string();
	}

public:
public:
};

} // namespace rest_api_objects
} // namespace duckdb
