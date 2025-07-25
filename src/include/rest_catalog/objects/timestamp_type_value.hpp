
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class TimestampTypeValue {
public:
	TimestampTypeValue();
	TimestampTypeValue(const TimestampTypeValue &) = delete;
	TimestampTypeValue &operator=(const TimestampTypeValue &) = delete;
	TimestampTypeValue(TimestampTypeValue &&) = default;
	TimestampTypeValue &operator=(TimestampTypeValue &&) = default;

public:
	static TimestampTypeValue FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	string value;
};

} // namespace rest_api_objects
} // namespace duckdb
