
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/table_identifier.hpp"
#include "rest_catalog/objects/table_requirement.hpp"
#include "rest_catalog/objects/table_update.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class CommitTableRequest {
public:
	CommitTableRequest::CommitTableRequest() {
	}

public:
	static CommitTableRequest FromJSON(yyjson_val *obj) {
		auto error = TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return *this;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		auto requirements_val = yyjson_obj_get(obj, "requirements");
		if (!requirements_val) {
		return "CommitTableRequest required property 'requirements' is missing");
		}
		size_t idx, max;
		yyjson_val *val;
		yyjson_arr_foreach(requirements_val, idx, max, val) {
			result.requirements.push_back(TableRequirement::FromJSON(val));
		}

		auto updates_val = yyjson_obj_get(obj, "updates");
		if (!updates_val) {
		return "CommitTableRequest required property 'updates' is missing");
		}
		size_t idx, max;
		yyjson_val *val;
		yyjson_arr_foreach(updates_val, idx, max, val) {
			result.updates.push_back(TableUpdate::FromJSON(val));
		}

		auto identifier_val = yyjson_obj_get(obj, "identifier");
		if (identifier_val) {
			result.identifier = TableIdentifier::FromJSON(identifier_val);
		}
		return string();
	}

public:
public:
};

} // namespace rest_api_objects
} // namespace duckdb
