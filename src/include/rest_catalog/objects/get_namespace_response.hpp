#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/namespace.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class GetNamespaceResponse {
public:
	static GetNamespaceResponse FromJSON(yyjson_val *obj) {
		GetNamespaceResponse result;

		auto _namespace_val = yyjson_obj_get(obj, "namespace");
		if (_namespace_val) {
			result._namespace = Namespace::FromJSON(_namespace_val);
		}
		else {
			throw IOException("GetNamespaceResponse required property 'namespace' is missing");
		}

		auto properties_val = yyjson_obj_get(obj, "properties");
		if (properties_val) {
			result.properties = parse_object_of_strings(properties_val);
		}

		return result;
	}

public:
	Namespace _namespace;
	ObjectOfStrings properties;
};
} // namespace rest_api_objects
} // namespace duckdb