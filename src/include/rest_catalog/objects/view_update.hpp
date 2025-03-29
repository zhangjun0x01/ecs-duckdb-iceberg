#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class ViewUpdate {
public:
	static ViewUpdate FromJSON(yyjson_val *obj) {
		ViewUpdate result;
		if (yyjson_is_obj(obj)) {
			if (yyjson_obj_get(obj, "uuid")) {
				result.assign_uuidupdate = AssignUUIDUpdate::FromJSON(obj);
				result.has_assign_uuidupdate = true;
			}
			if (yyjson_obj_get(obj, "format-version")) {
				result.upgrade_format_version_update = UpgradeFormatVersionUpdate::FromJSON(obj);
				result.has_upgrade_format_version_update = true;
			}
			if (yyjson_obj_get(obj, "schema")) {
				result.add_schema_update = AddSchemaUpdate::FromJSON(obj);
				result.has_add_schema_update = true;
			}
			if (yyjson_obj_get(obj, "location")) {
				result.set_location_update = SetLocationUpdate::FromJSON(obj);
				result.has_set_location_update = true;
			}
			if (yyjson_obj_get(obj, "updates")) {
				result.set_properties_update = SetPropertiesUpdate::FromJSON(obj);
				result.has_set_properties_update = true;
			}
			if (yyjson_obj_get(obj, "removals")) {
				result.remove_properties_update = RemovePropertiesUpdate::FromJSON(obj);
				result.has_remove_properties_update = true;
			}
			if (yyjson_obj_get(obj, "view-version")) {
				result.add_view_version_update = AddViewVersionUpdate::FromJSON(obj);
				result.has_add_view_version_update = true;
			}
			if (yyjson_obj_get(obj, "view-version-id")) {
				result.set_current_view_version_update = SetCurrentViewVersionUpdate::FromJSON(obj);
				result.has_set_current_view_version_update = true;
			}
			if (!(result.has_assign_uuidupdate || result.has_upgrade_format_version_update ||
			      result.has_add_schema_update || result.has_set_location_update || result.has_set_properties_update ||
			      result.has_remove_properties_update || result.has_add_view_version_update ||
			      result.has_set_current_view_version_update)) {
				throw IOException("ViewUpdate failed to parse, none of the accepted schemas found");
			}
		} else {
			throw IOException("ViewUpdate must be an object");
		}
		return result;
	}

public:
	AssignUUIDUpdate assign_uuidupdate;
	bool has_assign_uuidupdate = false;
	UpgradeFormatVersionUpdate upgrade_format_version_update;
	bool has_upgrade_format_version_update = false;
	AddSchemaUpdate add_schema_update;
	bool has_add_schema_update = false;
	SetLocationUpdate set_location_update;
	bool has_set_location_update = false;
	SetPropertiesUpdate set_properties_update;
	bool has_set_properties_update = false;
	RemovePropertiesUpdate remove_properties_update;
	bool has_remove_properties_update = false;
	AddViewVersionUpdate add_view_version_update;
	bool has_add_view_version_update = false;
	SetCurrentViewVersionUpdate set_current_view_version_update;
	bool has_set_current_view_version_update = false;
};
} // namespace rest_api_objects
} // namespace duckdb
