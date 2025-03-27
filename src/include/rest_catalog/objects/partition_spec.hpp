#pragma once

#include "yyjson.hpp"
#include <string>
#include <vector>
#include <unordered_map>
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/partition_field.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class PartitionSpec {
public:
	static PartitionSpec FromJSON(yyjson_val *obj) {
		PartitionSpec result;
		auto spec_id_val = yyjson_obj_get(obj, "spec-id");
		if (spec_id_val) {
			result.spec_id = yyjson_get_sint(spec_id_val);
		}
		auto fields_val = yyjson_obj_get(obj, "fields");
		if (fields_val) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(fields_val, idx, max, val) {
				result.fields.push_back(PartitionField::FromJSON(val));
			}
		}
		return result;
	}
public:
	int64_t spec_id;
	vector<PartitionField> fields;
};

} // namespace rest_api_objects
} // namespace duckdb