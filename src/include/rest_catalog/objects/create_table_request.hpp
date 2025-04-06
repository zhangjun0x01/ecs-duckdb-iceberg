
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/partition_spec.hpp"
#include "rest_catalog/objects/schema.hpp"
#include "rest_catalog/objects/sort_order.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class CreateTableRequest {
public:
	CreateTableRequest::CreateTableRequest() {
	}

public:
	static CreateTableRequest FromJSON(yyjson_val *obj) {
		auto error = TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return *this;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		auto name_val = yyjson_obj_get(obj, "name");
		if (!name_val) {
		return "CreateTableRequest required property 'name' is missing");
		}
		name = yyjson_get_str(name_val);

		auto schema_val = yyjson_obj_get(obj, "schema");
		if (!schema_val) {
		return "CreateTableRequest required property 'schema' is missing");
		}
		error = schema.TryFromJSON(schema_val);
		if (!error.empty()) {
			return error;
		}

		auto location_val = yyjson_obj_get(obj, "location");
		if (location_val) {
			location = yyjson_get_str(location_val);
		}

		auto partition_spec_val = yyjson_obj_get(obj, "partition_spec");
		if (partition_spec_val) {
			error = partition_spec.TryFromJSON(partition_spec_val);
			if (!error.empty()) {
				return error;
			}
		}

		auto write_order_val = yyjson_obj_get(obj, "write_order");
		if (write_order_val) {
			error = write_order.TryFromJSON(write_order_val);
			if (!error.empty()) {
				return error;
			}
		}

		auto stage_create_val = yyjson_obj_get(obj, "stage_create");
		if (stage_create_val) {
			stage_create = yyjson_get_bool(stage_create_val);
		}

		auto properties_val = yyjson_obj_get(obj, "properties");
		if (properties_val) {
			properties = parse_object_of_strings(properties_val);
		}
		return string();
	}

public:
public:
	string location;
	string name;
	PartitionSpec partition_spec;
	case_insensitive_map_t<string> properties;
	Schema schema;
	bool stage_create;
	SortOrder write_order;
};

} // namespace rest_api_objects
} // namespace duckdb
