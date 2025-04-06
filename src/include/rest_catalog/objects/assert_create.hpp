
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/table_requirement.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class AssertCreate {
public:
	AssertCreate::AssertCreate() {
	}

public:
	static AssertCreate FromJSON(yyjson_val *obj) {
		auto error = TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return *this;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		error = base_table_requirement.TryFromJSON(obj);
		if (!error.empty()) {
			return error;
		}

		auto type_val = yyjson_obj_get(obj, "type");
		if (!type_val) {
		return "AssertCreate required property 'type' is missing");
		}
		result.type = yyjson_get_str(type_val);

		return string();
	}

public:
	TableRequirement table_requirement;

public:
	string type;
};

} // namespace rest_api_objects
} // namespace duckdb
