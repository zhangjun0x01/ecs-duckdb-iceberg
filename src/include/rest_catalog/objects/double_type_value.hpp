
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class DoubleTypeValue {
public:
	DoubleTypeValue();
	DoubleTypeValue(const DoubleTypeValue &) = delete;
	DoubleTypeValue &operator=(const DoubleTypeValue &) = delete;
	DoubleTypeValue(DoubleTypeValue &&) = default;
	DoubleTypeValue &operator=(DoubleTypeValue &&) = default;

public:
	static DoubleTypeValue FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	double value;
};

} // namespace rest_api_objects
} // namespace duckdb
