
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/table_identifier.hpp"
#include "rest_catalog/objects/view_requirement.hpp"
#include "rest_catalog/objects/view_update.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class CommitViewRequest {
public:
	CommitViewRequest::CommitViewRequest() {
	}

public:
	static CommitViewRequest FromJSON(yyjson_val *obj) {
		auto error = TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return *this;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		auto updates_val = yyjson_obj_get(obj, "updates");
		if (!updates_val) {
		return "CommitViewRequest required property 'updates' is missing");
		}
		size_t idx, max;
		yyjson_val *val;
		yyjson_arr_foreach(updates_val, idx, max, val) {

			ViewUpdate tmp;
			error = tmp.TryFromJSON(val);
			if (!error.empty()) {
				return error;
			}
			updates.push_back(tmp);
		}

		auto identifier_val = yyjson_obj_get(obj, "identifier");
		if (identifier_val) {
			error = table_identifier.TryFromJSON(identifier_val);
			if (!error.empty()) {
				return error;
			}
		}

		auto requirements_val = yyjson_obj_get(obj, "requirements");
		if (requirements_val) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(requirements_val, idx, max, val) {

				ViewRequirement tmp;
				error = tmp.TryFromJSON(val);
				if (!error.empty()) {
					return error;
				}
				requirements.push_back(tmp);
			}
		}
		return string();
	}

public:
public:
	TableIdentifier identifier;
	vector<ViewRequirement> requirements;
	vector<ViewUpdate> updates;
};

} // namespace rest_api_objects
} // namespace duckdb
