
#include "rest_catalog/objects/partition_field.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

PartitionField::PartitionField() {
}

PartitionField PartitionField::FromJSON(yyjson_val *obj) {
	PartitionField res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string PartitionField::TryFromJSON(yyjson_val *obj) {
	string error;
	auto source_id_val = yyjson_obj_get(obj, "source-id");
	if (!source_id_val) {
		return "PartitionField required property 'source-id' is missing";
	} else {
		if (yyjson_is_sint(source_id_val)) {
			source_id = yyjson_get_sint(source_id_val);
		} else {
			return "PartitionField property 'source_id' is not of type 'integer'";
		}
	}
	auto transform_val = yyjson_obj_get(obj, "transform");
	if (!transform_val) {
		return "PartitionField required property 'transform' is missing";
	} else {
		error = transform.TryFromJSON(transform_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto name_val = yyjson_obj_get(obj, "name");
	if (!name_val) {
		return "PartitionField required property 'name' is missing";
	} else {
		if (yyjson_is_str(name_val)) {
			name = yyjson_get_str(name_val);
		} else {
			return "PartitionField property 'name' is not of type 'string'";
		}
	}
	auto field_id_val = yyjson_obj_get(obj, "field-id");
	if (field_id_val) {
		has_field_id = true;
		if (yyjson_is_sint(field_id_val)) {
			field_id = yyjson_get_sint(field_id_val);
		} else {
			return "PartitionField property 'field_id' is not of type 'integer'";
		}
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
