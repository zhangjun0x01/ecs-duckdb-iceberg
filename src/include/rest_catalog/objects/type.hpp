
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list_type.hpp"
#include "rest_catalog/objects/map_type.hpp"
#include "rest_catalog/objects/primitive_type.hpp"
#include "rest_catalog/objects/struct_type.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class Type {
public:
	Type::Type() {
	}

public:
	static Type FromJSON(yyjson_val *obj) {
		auto error = TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return *this;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;
		do {
			error = primitive_type.TryFromJSON(obj);
			if (error.empty()) {
				has_primitive_type = true;
				break;
			}
			error = struct_type.TryFromJSON(obj);
			if (error.empty()) {
				has_struct_type = true;
				break;
			}
			error = list_type.TryFromJSON(obj);
			if (error.empty()) {
				has_list_type = true;
				break;
			}
			error = map_type.TryFromJSON(obj);
			if (error.empty()) {
				has_map_type = true;
				break;
			}
			return "Type failed to parse, none of the oneOf candidates matched";
		} while (false);

		return string();
	}

public:
	PrimitiveType primitive_type;
	StructType struct_type;
	ListType list_type;
	MapType map_type;

public:
	bool has_primitive_type = false;
	bool has_struct_type = false;
	bool has_list_type = false;
	bool has_map_type = false;
};

} // namespace rest_api_objects
} // namespace duckdb
