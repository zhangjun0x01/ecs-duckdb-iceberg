#pragma once

#include "yyjson.hpp"
#include <string>
#include <vector>
#include <unordered_map>
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/view_version.hpp"
#include "rest_catalog/objects/schema.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class CreateViewRequest {
public:
	static CreateViewRequest FromJSON(yyjson_val *obj) {
		CreateViewRequest result;

		auto name_val = yyjson_obj_get(obj, "name");
		if (name_val) {
			result.name = yyjson_get_str(name_val);
		}
		else {
			throw IOException("CreateViewRequest required property 'name' is missing");
		}

		auto location_val = yyjson_obj_get(obj, "location");
		if (location_val) {
			result.location = yyjson_get_str(location_val);
		}

		auto schema_val = yyjson_obj_get(obj, "schema");
		if (schema_val) {
			result.schema = Schema::FromJSON(schema_val);
		}
		else {
			throw IOException("CreateViewRequest required property 'schema' is missing");
		}

		auto view_version_val = yyjson_obj_get(obj, "view-version");
		if (view_version_val) {
			result.view_version = ViewVersion::FromJSON(view_version_val);
		}
		else {
			throw IOException("CreateViewRequest required property 'view-version' is missing");
		}

		auto properties_val = yyjson_obj_get(obj, "properties");
		if (properties_val) {
			result.properties = parse_object_of_strings(properties_val);
		}
		else {
			throw IOException("CreateViewRequest required property 'properties' is missing");
		}

		return result;
	}

public:
	string name;
	string location;
	Schema schema;
	ViewVersion view_version;
	ObjectOfStrings properties;
};
} // namespace rest_api_objects
} // namespace duckdb