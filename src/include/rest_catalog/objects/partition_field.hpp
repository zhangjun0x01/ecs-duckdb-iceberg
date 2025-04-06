
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/transform.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class PartitionField {
public:
	PartitionField::PartitionField() {
	}

public:
	static PartitionField FromJSON(yyjson_val *obj) {
		auto error = TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return *this;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		auto source_id_val = yyjson_obj_get(obj, "source_id");
		if (!source_id_val) {
		return "PartitionField required property 'source_id' is missing");
		}
		result.source_id = yyjson_get_sint(source_id_val);

		auto transform_val = yyjson_obj_get(obj, "transform");
		if (!transform_val) {
		return "PartitionField required property 'transform' is missing");
		}
		result.transform = Transform::FromJSON(transform_val);

		auto name_val = yyjson_obj_get(obj, "name");
		if (!name_val) {
		return "PartitionField required property 'name' is missing");
		}
		result.name = yyjson_get_str(name_val);

		auto field_id_val = yyjson_obj_get(obj, "field_id");
		if (field_id_val) {
			result.field_id = yyjson_get_sint(field_id_val);
		}
		return string();
	}

public:
public:
	int64_t field_id;
	int64_t source_id;
	string name;
	Transform transform;
};

} // namespace rest_api_objects
} // namespace duckdb
