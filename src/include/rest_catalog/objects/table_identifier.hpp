#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/namespace.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class TableIdentifier {
public:
	static TableIdentifier FromJSON(yyjson_val *obj) {
		TableIdentifier result;

		auto name_val = yyjson_obj_get(obj, "name");
		if (name_val) {
			result.name = yyjson_get_str(name_val);
		} else {
			throw IOException("TableIdentifier required property 'name' is missing");
		}

		auto _namespace_val = yyjson_obj_get(obj, "namespace");
		if (_namespace_val) {
			result._namespace = Namespace::FromJSON(_namespace_val);
		} else {
			throw IOException("TableIdentifier required property 'namespace' is missing");
		}

		return result;
	}

public:
	string name;
	Namespace _namespace;
};
} // namespace rest_api_objects
} // namespace duckdb
