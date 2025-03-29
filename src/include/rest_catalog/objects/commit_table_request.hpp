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
	static CommitTableRequest FromJSON(yyjson_val *obj) {
		CommitTableRequest result;

		auto identifier_val = yyjson_obj_get(obj, "identifier");
		if (identifier_val) {
			result.identifier = TableIdentifier::FromJSON(identifier_val);
		}

		auto requirements_val = yyjson_obj_get(obj, "requirements");
		if (requirements_val) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(requirements_val, idx, max, val) {
				result.requirements.push_back(TableRequirement::FromJSON(val));
			}
		} else {
			throw IOException("CommitTableRequest required property 'requirements' is missing");
		}

		auto updates_val = yyjson_obj_get(obj, "updates");
		if (updates_val) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(updates_val, idx, max, val) {
				result.updates.push_back(TableUpdate::FromJSON(val));
			}
		} else {
			throw IOException("CommitTableRequest required property 'updates' is missing");
		}

		return result;
	}

public:
	TableIdentifier identifier;
	vector<TableRequirement> requirements;
	vector<TableUpdate> updates;
};
} // namespace rest_api_objects
} // namespace duckdb
