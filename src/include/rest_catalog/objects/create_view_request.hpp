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
	static CreateViewRequest FromJSON(yyjson_val *obj) {
		CreateViewRequest result;

		auto location_val = yyjson_obj_get(obj, "location");
		if (location_val) {
			result.location = yyjson_get_str(location_val);
		}

		auto name_val = yyjson_obj_get(obj, "name");
		if (name_val) {
			result.name = yyjson_get_str(name_val);
		} else {
			throw IOException("CreateViewRequest required property 'name' is missing");
		}

		auto properties_val = yyjson_obj_get(obj, "properties");
		if (properties_val) {
			result.properties = parse_object_of_strings(properties_val);
		} else {
			throw IOException("CreateViewRequest required property 'properties' is missing");
		}

		auto schema_val = yyjson_obj_get(obj, "schema");
		if (schema_val) {
			result.schema = Schema::FromJSON(schema_val);
		} else {
			throw IOException("CreateViewRequest required property 'schema' is missing");
		}

		auto view_version_val = yyjson_obj_get(obj, "view-version");
		if (view_version_val) {
			result.view_version = ViewVersion::FromJSON(view_version_val);
		} else {
			throw IOException("CreateViewRequest required property 'view-version' is missing");
		}

		return result;
	}

public:
	string location;
	string name;
	case_insensitive_map_t<string> properties;
	Schema schema;
	ViewVersion view_version;
};
} // namespace rest_api_objects
} // namespace duckdb
