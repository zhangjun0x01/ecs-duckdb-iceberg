
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
	TableIdentifier::TableIdentifier() {
	}

public:
	static TableIdentifier FromJSON(yyjson_val *obj) {
		auto error = TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return *this;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		auto _namespace_val = yyjson_obj_get(obj, "_namespace");
		if (!_namespace_val) {
		return "TableIdentifier required property '_namespace' is missing");
		}
		result._namespace = Namespace::FromJSON(_namespace_val);

		auto name_val = yyjson_obj_get(obj, "name");
		if (!name_val) {
		return "TableIdentifier required property 'name' is missing");
		}
		result.name = yyjson_get_str(name_val);

		return string();
	}

public:
public:
	Namespace _namespace;
	string name;
};

} // namespace rest_api_objects
} // namespace duckdb
