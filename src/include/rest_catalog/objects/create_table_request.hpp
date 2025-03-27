#pragma once

#include "yyjson.hpp"
#include <string>
#include <vector>
#include <unordered_map>
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/sort_order.hpp"
#include "rest_catalog/objects/partition_spec.hpp"
#include "rest_catalog/objects/schema.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class CreateTableRequest {
public:
	static CreateTableRequest FromJSON(yyjson_val *obj) {
		CreateTableRequest result;

		auto name_val = yyjson_obj_get(obj, "name");
		if (name_val) {
			result.name = yyjson_get_str(name_val);
		}
		else {
			throw IOException("CreateTableRequest required property 'name' is missing");
		}

		auto location_val = yyjson_obj_get(obj, "location");
		if (location_val) {
			result.location = yyjson_get_str(location_val);
		}

		auto schema_val = yyjson_obj_get(obj, "schema");
		if (schema_val) {
			result.schema = Schema::FromJSON(schema_val);
		}
		else {
			throw IOException("CreateTableRequest required property 'schema' is missing");
		}

		auto partition_spec_val = yyjson_obj_get(obj, "partition-spec");
		if (partition_spec_val) {
			result.partition_spec = PartitionSpec::FromJSON(partition_spec_val);
		}

		auto write_order_val = yyjson_obj_get(obj, "write-order");
		if (write_order_val) {
			result.write_order = SortOrder::FromJSON(write_order_val);
		}

		auto stage_create_val = yyjson_obj_get(obj, "stage-create");
		if (stage_create_val) {
			result.stage_create = yyjson_get_bool(stage_create_val);
		}

		auto properties_val = yyjson_obj_get(obj, "properties");
		if (properties_val) {
			result.properties = parse_object_of_strings(properties_val);
		}

		return result;
	}

public:
	string name;
	string location;
	Schema schema;
	PartitionSpec partition_spec;
	SortOrder write_order;
	bool stage_create;
	ObjectOfStrings properties;
};
} // namespace rest_api_objects
} // namespace duckdb