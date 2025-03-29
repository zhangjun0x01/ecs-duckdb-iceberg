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

class AssertTableUUID {
public:
	static AssertTableUUID FromJSON(yyjson_val *obj) {
		AssertTableUUID result;

		// Parse TableRequirement fields
		result.table_requirement = TableRequirement::FromJSON(obj);

		auto type_val = yyjson_obj_get(obj, "type");
		if (type_val) {
			result.type = yyjson_get_str(type_val);
		} else {
			throw IOException("AssertTableUUID required property 'type' is missing");
		}

		auto uuid_val = yyjson_obj_get(obj, "uuid");
		if (uuid_val) {
			result.uuid = yyjson_get_str(uuid_val);
		} else {
			throw IOException("AssertTableUUID required property 'uuid' is missing");
		}

		return result;
	}

public:
	TableRequirement table_requirement;
	string type;
	string uuid;
};
} // namespace rest_api_objects
} // namespace duckdb
