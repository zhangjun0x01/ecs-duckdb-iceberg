
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/base_update.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class SetLocationUpdate {
public:
	SetLocationUpdate();
	SetLocationUpdate(const SetLocationUpdate &) = delete;
	SetLocationUpdate &operator=(const SetLocationUpdate &) = delete;
	SetLocationUpdate(SetLocationUpdate &&) = default;
	SetLocationUpdate &operator=(SetLocationUpdate &&) = default;

public:
	static SetLocationUpdate FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	BaseUpdate base_update;
	string location;
	string action;
	bool has_action = false;
};

} // namespace rest_api_objects
} // namespace duckdb
