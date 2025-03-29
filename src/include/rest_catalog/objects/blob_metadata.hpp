#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class BlobMetadata {
public:
	static BlobMetadata FromJSON(yyjson_val *obj) {
		BlobMetadata result;

		auto fields_val = yyjson_obj_get(obj, "fields");
		if (fields_val) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(fields_val, idx, max, val) {
				result.fields.push_back(yyjson_get_sint(val));
			}
		} else {
			throw IOException("BlobMetadata required property 'fields' is missing");
		}

		auto properties_val = yyjson_obj_get(obj, "properties");
		if (properties_val) {
			result.properties = parse_object_of_strings(properties_val);
		}

		auto sequence_number_val = yyjson_obj_get(obj, "sequence-number");
		if (sequence_number_val) {
			result.sequence_number = yyjson_get_sint(sequence_number_val);
		} else {
			throw IOException("BlobMetadata required property 'sequence-number' is missing");
		}

		auto snapshot_id_val = yyjson_obj_get(obj, "snapshot-id");
		if (snapshot_id_val) {
			result.snapshot_id = yyjson_get_sint(snapshot_id_val);
		} else {
			throw IOException("BlobMetadata required property 'snapshot-id' is missing");
		}

		auto type_val = yyjson_obj_get(obj, "type");
		if (type_val) {
			result.type = yyjson_get_str(type_val);
		} else {
			throw IOException("BlobMetadata required property 'type' is missing");
		}

		return result;
	}

public:
	vector<int64_t> fields;
	case_insensitive_map_t<string> properties;
	int64_t sequence_number;
	int64_t snapshot_id;
	string type;
};
} // namespace rest_api_objects
} // namespace duckdb
