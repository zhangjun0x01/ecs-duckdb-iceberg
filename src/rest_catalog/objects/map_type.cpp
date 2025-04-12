
#include "rest_catalog/objects/map_type.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

MapType::MapType() {
}

MapType MapType::FromJSON(yyjson_val *obj) {
	MapType res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string MapType::TryFromJSON(yyjson_val *obj) {
	string error;
	auto type_val = yyjson_obj_get(obj, "type");
	if (!type_val) {
		return "MapType required property 'type' is missing";
	} else {
		if (yyjson_is_str(type_val)) {
			type = yyjson_get_str(type_val);
		} else {
			return "MapType property 'type' is not of type 'string'";
		}
	}
	auto key_id_val = yyjson_obj_get(obj, "key-id");
	if (!key_id_val) {
		return "MapType required property 'key-id' is missing";
	} else {
		if (yyjson_is_sint(key_id_val)) {
			key_id = yyjson_get_sint(key_id_val);
		} else {
			return "MapType property 'key_id' is not of type 'integer'";
		}
	}
	auto key_val = yyjson_obj_get(obj, "key");
	if (!key_val) {
		return "MapType required property 'key' is missing";
	} else {
		key = make_uniq<Type>();
		error = key->TryFromJSON(key_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto value_id_val = yyjson_obj_get(obj, "value-id");
	if (!value_id_val) {
		return "MapType required property 'value-id' is missing";
	} else {
		if (yyjson_is_sint(value_id_val)) {
			value_id = yyjson_get_sint(value_id_val);
		} else {
			return "MapType property 'value_id' is not of type 'integer'";
		}
	}
	auto value_val = yyjson_obj_get(obj, "value");
	if (!value_val) {
		return "MapType required property 'value' is missing";
	} else {
		value = make_uniq<Type>();
		error = value->TryFromJSON(value_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto value_required_val = yyjson_obj_get(obj, "value-required");
	if (!value_required_val) {
		return "MapType required property 'value-required' is missing";
	} else {
		if (yyjson_is_bool(value_required_val)) {
			value_required = yyjson_get_bool(value_required_val);
		} else {
			return "MapType property 'value_required' is not of type 'boolean'";
		}
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
