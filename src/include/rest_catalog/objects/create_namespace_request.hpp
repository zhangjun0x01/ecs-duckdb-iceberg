
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

class CreateNamespaceRequest {
public:
	CreateNamespaceRequest() {
	}

public:
	static CreateNamespaceRequest FromJSON(yyjson_val *obj) {
		CreateNamespaceRequest res;
		auto error = res.TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return res;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;
		auto _namespace_val = yyjson_obj_get(obj, "_namespace");
		if (!_namespace_val) {
			return "CreateNamespaceRequest required property '_namespace' is missing";
		} else {
			error = _namespace.TryFromJSON(_namespace_val);
			if (!error.empty()) {
				return error;
			}
		}
		auto properties_val = yyjson_obj_get(obj, "properties");
		if (properties_val) {
			properties = parse_object_of_strings(properties_val);
		}
		return string();
	}

public:
	Namespace _namespace;
	case_insensitive_map_t<string> properties;
};

} // namespace rest_api_objects
} // namespace duckdb
