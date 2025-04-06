
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/partition_field.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class PartitionSpec {
public:
	PartitionSpec() {
	}

public:
	static PartitionSpec FromJSON(yyjson_val *obj) {
		PartitionSpec res;
		auto error = res.TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return res;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;
		auto fields_val = yyjson_obj_get(obj, "fields");
		if (!fields_val) {
			return "PartitionSpec required property 'fields' is missing";
		} else {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(fields_val, idx, max, val) {
				PartitionField tmp;
				error = tmp.TryFromJSON(val);
				if (!error.empty()) {
					return error;
				}
				fields.push_back(tmp);
			}
		}
		auto spec_id_val = yyjson_obj_get(obj, "spec_id");
		if (spec_id_val) {
			spec_id = yyjson_get_sint(spec_id_val);
		}
		return string();
	}

public:
	vector<PartitionField> fields;
	int64_t spec_id;
};

} // namespace rest_api_objects
} // namespace duckdb
