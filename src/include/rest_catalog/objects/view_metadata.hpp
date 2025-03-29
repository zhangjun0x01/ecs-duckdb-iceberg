#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/schema.hpp"
#include "rest_catalog/objects/view_history_entry.hpp"
#include "rest_catalog/objects/view_version.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class ViewMetadata {
public:
	static ViewMetadata FromJSON(yyjson_val *obj) {
		ViewMetadata result;

		auto current_version_id_val = yyjson_obj_get(obj, "current-version-id");
		if (current_version_id_val) {
			result.current_version_id = yyjson_get_sint(current_version_id_val);
		} else {
			throw IOException("ViewMetadata required property 'current-version-id' is missing");
		}

		auto format_version_val = yyjson_obj_get(obj, "format-version");
		if (format_version_val) {
			result.format_version = yyjson_get_sint(format_version_val);
		} else {
			throw IOException("ViewMetadata required property 'format-version' is missing");
		}

		auto location_val = yyjson_obj_get(obj, "location");
		if (location_val) {
			result.location = yyjson_get_str(location_val);
		} else {
			throw IOException("ViewMetadata required property 'location' is missing");
		}

		auto properties_val = yyjson_obj_get(obj, "properties");
		if (properties_val) {
			result.properties = parse_object_of_strings(properties_val);
		}

		auto schemas_val = yyjson_obj_get(obj, "schemas");
		if (schemas_val) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(schemas_val, idx, max, val) {
				result.schemas.push_back(Schema::FromJSON(val));
			}
		} else {
			throw IOException("ViewMetadata required property 'schemas' is missing");
		}

		auto version_log_val = yyjson_obj_get(obj, "version-log");
		if (version_log_val) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(version_log_val, idx, max, val) {
				result.version_log.push_back(ViewHistoryEntry::FromJSON(val));
			}
		} else {
			throw IOException("ViewMetadata required property 'version-log' is missing");
		}

		auto versions_val = yyjson_obj_get(obj, "versions");
		if (versions_val) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(versions_val, idx, max, val) {
				result.versions.push_back(ViewVersion::FromJSON(val));
			}
		} else {
			throw IOException("ViewMetadata required property 'versions' is missing");
		}

		auto view_uuid_val = yyjson_obj_get(obj, "view-uuid");
		if (view_uuid_val) {
			result.view_uuid = yyjson_get_str(view_uuid_val);
		} else {
			throw IOException("ViewMetadata required property 'view-uuid' is missing");
		}

		return result;
	}

public:
	int64_t current_version_id;
	int64_t format_version;
	string location;
	case_insensitive_map_t<string> properties;
	vector<Schema> schemas;
	vector<ViewHistoryEntry> version_log;
	vector<ViewVersion> versions;
	string view_uuid;
};
} // namespace rest_api_objects
} // namespace duckdb
