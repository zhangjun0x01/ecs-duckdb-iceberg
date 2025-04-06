
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

class CreateNamespaceResponse {
public:
	CreateNamespaceResponse::CreateNamespaceResponse() {
	}

public:
	static CreateNamespaceResponse FromJSON(yyjson_val *obj) {
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
		return "CreateNamespaceResponse required property '_namespace' is missing");
		}
		result._namespace = Namespace::FromJSON(_namespace_val);

		auto properties_val = yyjson_obj_get(obj, "properties");
		if (properties_val) {
			result.properties = parse_object_of_strings(properties_val);
			;
		}
		return string();
	}

public:
public:
};

} // namespace rest_api_objects
} // namespace duckdb
