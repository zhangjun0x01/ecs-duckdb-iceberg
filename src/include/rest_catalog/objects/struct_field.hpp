
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/primitive_type_value.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class Type;

class StructField {
public:
	StructField();

public:
	static StructField FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	int64_t id;
	string name;
	unique_ptr<Type> type;
	bool required;
	string doc;
	PrimitiveTypeValue initial_default;
	PrimitiveTypeValue write_default;
};

} // namespace rest_api_objects
} // namespace duckdb
