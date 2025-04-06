
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
	PartitionSpec::PartitionSpec() {
	}

public:
	static PartitionSpec FromJSON(yyjson_val *obj) {
		auto error = TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return *this;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		auto fields_val = yyjson_obj_get(obj, "fields");
		if (!fields_val) {
		return "PartitionSpec required property 'fields' is missing");
		}
		size_t idx, max;
		yyjson_val *val;
		yyjson_arr_foreach(fields_val, idx, max, val) {
			result.fields.push_back(PartitionField::FromJSON(val));
		}

		auto spec_id_val = yyjson_obj_get(obj, "spec_id");
		if (spec_id_val) {
			result.spec_id = yyjson_get_sint(spec_id_val);
		}
		return string();
	}

public:
public:
	vector<PartitionField> fields;
	int64_t spec_id;
};

} // namespace rest_api_objects
} // namespace duckdb
