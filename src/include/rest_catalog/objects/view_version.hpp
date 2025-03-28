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
	static ViewVersion FromJSON(yyjson_val *obj) {
		ViewVersion result;

		auto default_catalog_val = yyjson_obj_get(obj, "default-catalog");
		if (default_catalog_val) {
			result.default_catalog = yyjson_get_str(default_catalog_val);
		}

		auto default_namespace_val = yyjson_obj_get(obj, "default-namespace");
		if (default_namespace_val) {
			result.default_namespace = Namespace::FromJSON(default_namespace_val);
		}
		else {
			throw IOException("ViewVersion required property 'default-namespace' is missing");
		}

		auto representations_val = yyjson_obj_get(obj, "representations");
		if (representations_val) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(representations_val, idx, max, val) {
				result.representations.push_back(ViewRepresentation::FromJSON(val));
			}
		}
		else {
			throw IOException("ViewVersion required property 'representations' is missing");
		}

		auto schema_id_val = yyjson_obj_get(obj, "schema-id");
		if (schema_id_val) {
			result.schema_id = yyjson_get_sint(schema_id_val);
		}
		else {
			throw IOException("ViewVersion required property 'schema-id' is missing");
		}

		auto summary_val = yyjson_obj_get(obj, "summary");
		if (summary_val) {
			result.summary = parse_object_of_strings(summary_val);
		}
		else {
			throw IOException("ViewVersion required property 'summary' is missing");
		}

		auto timestamp_ms_val = yyjson_obj_get(obj, "timestamp-ms");
		if (timestamp_ms_val) {
			result.timestamp_ms = yyjson_get_sint(timestamp_ms_val);
		}
		else {
			throw IOException("ViewVersion required property 'timestamp-ms' is missing");
		}

		auto version_id_val = yyjson_obj_get(obj, "version-id");
		if (version_id_val) {
			result.version_id = yyjson_get_sint(version_id_val);
		}
		else {
			throw IOException("ViewVersion required property 'version-id' is missing");
		}

		return result;
	}

public:
	string default_catalog;
	Namespace default_namespace;
	vector<ViewRepresentation> representations;
	int64_t schema_id;
	case_insensitive_map_t<string> summary;
	int64_t timestamp_ms;
	int64_t version_id;
};
} // namespace rest_api_objects
} // namespace duckdb