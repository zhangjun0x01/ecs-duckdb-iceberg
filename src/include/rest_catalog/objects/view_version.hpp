
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/namespace.hpp"
#include "rest_catalog/objects/view_representation.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class ViewVersion {
public:
	ViewVersion::ViewVersion() {
	}

public:
	static ViewVersion FromJSON(yyjson_val *obj) {
		auto error = TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return *this;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		auto version_id_val = yyjson_obj_get(obj, "version_id");
		if (!version_id_val) {
		return "ViewVersion required property 'version_id' is missing");
		}
		result.version_id = yyjson_get_sint(version_id_val);

		auto timestamp_ms_val = yyjson_obj_get(obj, "timestamp_ms");
		if (!timestamp_ms_val) {
		return "ViewVersion required property 'timestamp_ms' is missing");
		}
		result.timestamp_ms = yyjson_get_sint(timestamp_ms_val);

		auto schema_id_val = yyjson_obj_get(obj, "schema_id");
		if (!schema_id_val) {
		return "ViewVersion required property 'schema_id' is missing");
		}
		result.schema_id = yyjson_get_sint(schema_id_val);

		auto summary_val = yyjson_obj_get(obj, "summary");
		if (!summary_val) {
		return "ViewVersion required property 'summary' is missing");
		}
		result.summary = parse_object_of_strings(summary_val);

		auto representations_val = yyjson_obj_get(obj, "representations");
		if (!representations_val) {
		return "ViewVersion required property 'representations' is missing");
		}
		size_t idx, max;
		yyjson_val *val;
		yyjson_arr_foreach(representations_val, idx, max, val) {
			result.representations.push_back(ViewRepresentation::FromJSON(val));
		}

		auto default_namespace_val = yyjson_obj_get(obj, "default_namespace");
		if (!default_namespace_val) {
		return "ViewVersion required property 'default_namespace' is missing");
		}
		result.default_namespace = Namespace::FromJSON(default_namespace_val);

		auto default_catalog_val = yyjson_obj_get(obj, "default_catalog");
		if (default_catalog_val) {
			result.default_catalog = yyjson_get_str(default_catalog_val);
			;
		}
		return string();
	}

public:
public:
};

} // namespace rest_api_objects
} // namespace duckdb
