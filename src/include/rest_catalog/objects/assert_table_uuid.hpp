#pragma once

#include "yyjson.hpp"
#include <string>
#include <vector>
#include <unordered_map>
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/table_requirement.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class AssertTableUUID {
public:
	static AssertTableUUID FromJSON(yyjson_val *obj) {
		AssertTableUUID result;
		auto type_val = yyjson_obj_get(obj, "type");
		if (type_val) {
			result.type = yyjson_get_str(type_val);
		}
		auto uuid_val = yyjson_obj_get(obj, "uuid");
		if (uuid_val) {
			result.uuid = yyjson_get_str(uuid_val);
		}
		return result;
	}
public:
	string type;
	string uuid;
};

} // namespace rest_api_objects
} // namespace duckdb