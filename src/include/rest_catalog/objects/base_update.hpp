
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class BaseUpdate {
public:
	BaseUpdate::BaseUpdate() {
	}

public:
	static BaseUpdate FromJSON(yyjson_val *obj) {
		auto error = TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return *this;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		auto action_val = yyjson_obj_get(obj, "action");
		if (!action_val) {
		return "BaseUpdate required property 'action' is missing");
		}
		action = yyjson_get_str(action_val);

		return string();
	}

public:
public:
	string action;
};

} // namespace rest_api_objects
} // namespace duckdb
