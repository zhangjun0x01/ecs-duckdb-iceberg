
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/add_schema_update.hpp"
#include "rest_catalog/objects/add_view_version_update.hpp"
#include "rest_catalog/objects/assign_uuidupdate.hpp"
#include "rest_catalog/objects/remove_properties_update.hpp"
#include "rest_catalog/objects/set_current_view_version_update.hpp"
#include "rest_catalog/objects/set_location_update.hpp"
#include "rest_catalog/objects/set_properties_update.hpp"
#include "rest_catalog/objects/upgrade_format_version_update.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class ViewUpdate {
public:
	ViewUpdate::ViewUpdate() {
	}

public:
	static ViewUpdate FromJSON(yyjson_val *obj) {
		auto error = TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return *this;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		error = base_assign_uuidupdate.TryFromJSON(obj);
		if (error.empty()) {
			has_assign_uuidupdate = true;
		}

		error = base_upgrade_format_version_update.TryFromJSON(obj);
		if (error.empty()) {
			has_upgrade_format_version_update = true;
		}

		error = base_add_schema_update.TryFromJSON(obj);
		if (error.empty()) {
			has_add_schema_update = true;
		}

		error = base_set_location_update.TryFromJSON(obj);
		if (error.empty()) {
			has_set_location_update = true;
		}

		error = base_set_properties_update.TryFromJSON(obj);
		if (error.empty()) {
			has_set_properties_update = true;
		}

		error = base_remove_properties_update.TryFromJSON(obj);
		if (error.empty()) {
			has_remove_properties_update = true;
		}

		error = base_add_view_version_update.TryFromJSON(obj);
		if (error.empty()) {
			has_add_view_version_update = true;
		}

		error = base_set_current_view_version_update.TryFromJSON(obj);
		if (error.empty()) {
			has_set_current_view_version_update = true;
		}

		if (!has_add_schema_update && !has_add_view_version_update && !has_assign_uuidupdate &&
		    !has_remove_properties_update && !has_set_current_view_version_update && !has_set_location_update &&
		    !has_set_properties_update && !has_upgrade_format_version_update) {
			return "ViewUpdate failed to parse, none of the anyOf candidates matched";
		}

		return string();
	}

public:
	SetPropertiesUpdate set_properties_update;
	SetLocationUpdate set_location_update;
	AddSchemaUpdate add_schema_update;
	AddViewVersionUpdate add_view_version_update;
	RemovePropertiesUpdate remove_properties_update;
	SetCurrentViewVersionUpdate set_current_view_version_update;
	UpgradeFormatVersionUpdate upgrade_format_version_update;
	AssignUUIDUpdate assign_uuidupdate;

public:
};

} // namespace rest_api_objects
} // namespace duckdb
