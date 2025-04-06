
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
	ViewMetadata() {
	}

public:
	static ViewMetadata FromJSON(yyjson_val *obj) {
		ViewMetadata res;
		auto error = res.TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return res;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		auto view_uuid_val = yyjson_obj_get(obj, "view_uuid");
		if (!view_uuid_val) {
			return "ViewMetadata required property 'view_uuid' is missing";
		} else {
			view_uuid = yyjson_get_str(view_uuid_val);
		}

		auto format_version_val = yyjson_obj_get(obj, "format_version");
		if (!format_version_val) {
			return "ViewMetadata required property 'format_version' is missing";
		} else {
			format_version = yyjson_get_sint(format_version_val);
		}

		auto location_val = yyjson_obj_get(obj, "location");
		if (!location_val) {
			return "ViewMetadata required property 'location' is missing";
		} else {
			location = yyjson_get_str(location_val);
		}

		auto current_version_id_val = yyjson_obj_get(obj, "current_version_id");
		if (!current_version_id_val) {
			return "ViewMetadata required property 'current_version_id' is missing";
		} else {
			current_version_id = yyjson_get_sint(current_version_id_val);
		}

		auto versions_val = yyjson_obj_get(obj, "versions");
		if (!versions_val) {
			return "ViewMetadata required property 'versions' is missing";
		} else {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(versions_val, idx, max, val) {

				ViewVersion tmp;
				error = tmp.TryFromJSON(val);
				if (!error.empty()) {
					return error;
				}
				versions.push_back(tmp);
			}
		}

		auto version_log_val = yyjson_obj_get(obj, "version_log");
		if (!version_log_val) {
			return "ViewMetadata required property 'version_log' is missing";
		} else {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(version_log_val, idx, max, val) {

				ViewHistoryEntry tmp;
				error = tmp.TryFromJSON(val);
				if (!error.empty()) {
					return error;
				}
				version_log.push_back(tmp);
			}
		}

		auto schemas_val = yyjson_obj_get(obj, "schemas");
		if (!schemas_val) {
			return "ViewMetadata required property 'schemas' is missing";
		} else {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(schemas_val, idx, max, val) {

				Schema tmp;
				error = tmp.TryFromJSON(val);
				if (!error.empty()) {
					return error;
				}
				schemas.push_back(tmp);
			}
		}

		auto properties_val = yyjson_obj_get(obj, "properties");
		if (properties_val) {
			properties = parse_object_of_strings(properties_val);
		}

		return string();
	}

public:
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
