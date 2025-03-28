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
	static AssertCreate FromJSON(yyjson_val *obj) {
		AssertCreate result;

		// Parse TableRequirement fields
		result.table_requirement = TableRequirement::FromJSON(obj);

		auto type_val = yyjson_obj_get(obj, "type");
		if (type_val) {
			result.type = yyjson_get_str(type_val);
		}
		else {
			throw IOException("AssertCreate required property 'type' is missing");
		}

		return result;
	}

public:
	TableRequirement table_requirement;
	string type;
};
} // namespace rest_api_objects
} // namespace duckdb